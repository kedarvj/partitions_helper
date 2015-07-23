#!/usr/bin/perl
#    The MySQL Partitions helper
#    Copyright (C) 2008, 2009 Giuseppe Maxia
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; version 2 of the License
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

#
# This program creates a ALTER TABLE statement to add or reorganize 
# date based partitions for MySQL 5.1 or later
#

use strict;
use warnings;
# use diagnostics;
use English qw( -no_match_vars ) ;
use Getopt::Long qw(:config no_ignore_case );
use Data::Dumper;

my $VERSION = '1.0.4';

#
# Parse options are fully qualified options with descriptive help,
# parse string for the command line, and sort order for the help
#
my %parse_options = (
    table           =>  {
                            value   => '',
                            parse   => 't|table=s',
                            help    => [
                                        'The table being partitioned',
                                        '(no default)'
                                       ],
                            so      =>  20,
                        },
    column          =>  {
                            value   => '',
                            parse   => 'c|column=s',
                            help    => [
                                        'The partitioning column',
                                        '(no default)',
                                       ],
                            so      =>  30,
                        },
    interval        =>  {
                            value   => 'month',
                            parse   => 'i|interval=s',
                            help    => [
                                        'The interval between partitions',
                                        'Accepted: "year", "month", "week", "day", or a number',
                                        '(default: month) ',
                                       ],
                            so      =>  40,
                        },
    partitions      =>  {
                            value   => 0,
                            parse   => 'p|partitions=i',
                            help    => [
                                        'How many partitions to create',
                                        '(default: 0) ',
                                       ],
                            so      =>  50,
                        },
    first_partition =>  {
                            value   => 1,
                            parse   => 'first_partition=i',
                            help    => [
                                        'the first partition to create',
                                        '(default: 1) ',
                                       ],
                            so      =>  60,
                        },
    reorganize      =>  {
                            value   => '',
                            parse   => 'reorganize=s',
                            help    => [
                                        'the partition(s) to reorganize',
                                        '(default: none) '
                                       ],
                            so      =>  70,
                        },
    
    start           =>  {
                            value   => '2001-01-01',
                            parse   => 's|start=s',
                            help    => [
                                        'the minimum partitioning value',
                                        '(default: 1 for numbers, 2001-01-01 for dates) '
                                       ],
                            so      =>  80,
                        },
    end             =>  {
                            value   => '',
                            parse   => 'e|end=s',
                            help    => [
                                        'The maximum partitioning value',
                                        'Used unless --partitions is used',
                                        '(no default) ',
                                       ],
                            so      =>  90,
                        },
    function             =>  {
                            value   => '',
                            parse   => 'f|function=s',
                            help    => [
                                        'The partitioning function to use in the "range" declaration',
                                        '(default: to_days, unless --list is used) ',
                                       ],
                            so      =>  100,
                        },
    list             =>  {
                            value   => 0,
                            parse   => 'l|list',
                            help    => [
                                        'Use the COLUMNS feature (versions >= 5.5)',
                                        '(default: no) ',
                                       ],
                            so      =>  110,
                        },
     maxvalue       =>  {
                            value   => 0,
                            parse   => 'x|maxvalue',
                            help    => [
                                        'Adds MAXVALUE as last partition',
                                        '(default: disabled) ',
                                       ],
                            so      =>  115,
                        },
    prefix           =>  {
                            value   => 'p',
                            parse   => 'prefix=s',
                            help    => [
                                        'prefix for partition name',
                                        '(default: p) ',
                                       ],
                            so      =>  120,
                        },
    explain          =>  {
                            value   => 0,
                            parse   => 'explain',
                            help    => [
                                        'show the current option values',
                                        '(default: no) ',
                                       ],
                            so      =>  130,
                        },

    version             =>  {
                            value   => 0,
                            parse   => 'version',
                            help    => [
                                        'display the version',
                                        '(default: none) ',
                                       ],
                            so      =>  400,
                        },
    help             =>  {
                            value   => 0,
                            parse   => 'h|help',
                            help    => [
                                        'display the help page',
                                        '(default: no) ',
                                       ],
                            so      =>  500,
                        },
);

# 
# convert parse options to simple options
#
my %options = map { $_ ,  $parse_options{$_}{'value'}}  keys %parse_options;

# 
# get the options from the command line
#
GetOptions (
    map { $parse_options{$_}{parse}, \$options{$_} }        
        grep { $parse_options{$_}{parse}}  keys %parse_options 
) or get_help();

get_help() if $options{help};

if ($options{version}) {
    print credits();
    exit 0;
}


# print Dumper(\%options) ; exit;

my %valid_intervals = (
    day   => 1,
    week  => 1,
    month => 1,
    year  => 1,
);

#
# start and end dates
#
my ($syear, $smonth, $sday) = ();
my ($eyear, $emonth, $eday) = ();

#
# deals with placeholder features
#

for my $op ( qw(operation) ) {
    if ($options{$op}) {
        die "option <$op> is not implemented yet\n";
    }
}

# 
# check that a table and column are given
#
unless ($options{table}) {
    die "table name required\n";
}

unless ($options{column} or $options{reorganize} ) {
    die "column name required\n";
}

#
# accept only one of --end or --partitions
#
if ($options{end} && $options{partitions}) {
    die "too many quantifiers. Use EITHER '--partitions' OR '--end' \n";
}

#
# check that we parsed a valid interval
#
if ( $options{interval} =~ /^\d+$/) {
    unless ($options{start} =~ /^\d+$/) {
        $options{start} = 1;
    }
    if ($options{end}) {
        unless ($options{end} =~ /^\d+$/) {
            die "the end value must be a number\n";
        } 
        if ($options{end} < $options{interval}) {
            die "the end value must be bigger than the interval\n";
        }
        if ($options{end} <= $options{start}) {
            die "the end value must be bigger than the start\n";
        }
        $options{partitions} = int( ($options{end} +1 - $options{start}) / $options{interval});
    }
}
else {
    unless (exists $valid_intervals{ $options{interval} } ) {
        die "invalid interval specified: $options{interval}\n";
    }
    #
    # for year,  month, or week the function must be to_days
    #
    unless ($options{list}) {
        $options{function} = 'to_days' unless $options{function};
    }

    #
    # check the start date
    #
    if ( $options{start} =~ /(\d{4})[\-\.\/](\d+)[\-\.\/](\d+)/) {
        ($syear, $smonth, $sday) = ($1, $2, $3);
        $smonth +=0;
        $sday +=0;
        #print "start $syear $smonth $sday\n";
    }
    else {
        die "invalid date $options{start}\n";
    }
    #
    # check the end date
    #
    if ($options{end}) {
        if ( $options{end} =~ /(\d{4})[\-\.\/](\d+)[\-\.\/](\d+)/) {
            ($eyear, $emonth, $eday) = ($1, $2, $3);
            $emonth +=0;
            $eday +=0;
            # print "end $eyear $emonth $eday\n";
        }
        else {
            die "invalid date $options{end}\n";
        }
        if ($options{interval} eq 'year') {
            $options{partitions} = $eyear - $syear +1;
        }
        elsif ($options{interval} eq 'month') {
            my $months =   (12 - $smonth) 
                 + ( ($eyear - $syear -1) * 12 )
                 + $emonth + 1;
            # print $months,$/;
            $options{partitions} = $months;
        }
        elsif ($options{interval} eq 'week') {
            $options{partitions} = weeks_between($syear, $smonth, $sday,
                                                 $eyear, $emonth, $eday);
        }
        elsif ($options{interval} eq 'day') {
            $options{partitions} = days_between($syear, $smonth, $sday,
                                                 $eyear, $emonth, $eday);
        }
    }       
}

#
# there must be a valid number of partitions
#

unless ($options{partitions} && ($options{partitions} =~ /^\d+$/) ) {
    die "number of partitions required. Use EITHER '--partitions' OR '--end'\n";
}

if ($options{partitions} > 1024) {
    die "too many partitions ($options{partitions}). Maximum allowed is 1024\n";
} 
else {
    print "# partitions: $options{partitions}\n";
}

#
# don't accept a function if COLUMS is being used
#
if ( $options{function} && $options{list} ) {
    die "you must specify either list or function\n";
}

if ($options{explain}) {
    show_options();
}

# print Dumper(\%options) ; exit;

# -----------------------------------------
# start building the ALTER TABLE statement
# -----------------------------------------

print "ALTER TABLE $options{table} \n";
if ($options{reorganize} ) {
    print "REORGANIZE PARTITIONS $options{reorganize} INTO \n";
    $options{prefix} = 'pr';
}
else {
    print "PARTITION by range " ;

    if ($options{function}) {
        print "($options{function}(";
    }
    elsif ($options{list}) {
        print "columns(" 
    }
    else {
        print "("
    }

    print "$options{column}";

    if ($options{function}) {
        print ")";
    }

    print ")\n";
}

print "(\n";

make_partitions( $options{interval});

print ");\n";


# -----------------------------------------
# functions
# -----------------------------------------

sub make_partitions {
    my ($interval) = @_;
    my $partitions_done=0;
    my $p_year  = $syear;
    my $p_month = $smonth;
    my $p_day = $sday;
    my $func_start = 'to_days('; #$options{function};
    my $func_end = ")";
    if ($options{list}) {
        $func_start = "";
        $func_end = "";
    }
    for my $part ($options{first_partition} .. 
            $options{first_partition} + $options{partitions} -1 ) {
        if ($partitions_done) {
            print ", ";
        }
        else {
            print "  "
        }
        $partitions_done++;

        if ($interval =~ /^\d+$/) {
            printf "partition %s%03d VALUES LESS THAN (%d)\n", 
                $options{prefix},
                $partitions_done,
                ($options{start} + (($partitions_done - 1) * $interval)) + $interval;
        }
        else {
            printf "partition %s%03d VALUES LESS THAN (%s'%4d-%02d-%02d'%s)\n", 
                $options{prefix},
                $partitions_done,
                $func_start,
                $p_year,
                $p_month,
                $p_day,
                $func_end;
            if ($interval eq 'day') {
                ($p_year,$p_month,$p_day) = next_day($p_year, $p_month, $p_day);
            }
            elsif ($interval eq 'week') {
                ($p_year,$p_month,$p_day) = next_week($p_year, $p_month, $p_day);
            }
            elsif ($interval eq 'month') {
                ($p_year,$p_month) = next_month($p_year, $p_month);
            }
            elsif ($interval eq 'year') {
                ($p_year,$p_month) = next_year($p_year, $p_month);
            }
            else {
                die "unsupported interval\n";
            }
        }
    }
    if ($options{'maxvalue'}) {
            printf ", partition %s%03d VALUES LESS THAN (MAXVALUE)\n", 
                $options{prefix},
                ++$partitions_done;
    }
}

sub next_year {
    my ($y, $m) = @_;
    $y++;
    return ($y, $m);
}

sub next_week {
    my ($y, $m, $d) = @_;
    for my $i (1 .. 7) {
        ($y, $m, $d) = next_day($y, $m, $d);
    }
    return ($y, $m, $d);
}

sub next_day {
    my ($y, $m, $d) = @_;
    $d++;
    $m += 0;
    my $last_day = days_in_month($y, $m);
    if ($d > $last_day) {
        $d = 1;
        $m++;
    }
    if ($m > 12) {
        $m = 1;
        $y++;
    }
    return ($y, $m, $d);
}

sub is_leap_year {
    my ($y) = @_;
    if (($y % 400) == 0) {
        return 1;
    }
    elsif (($y % 100) == 0) {
        return 0;
    }
    elsif (($y % 4) == 0) {
        return 1
    }
    return 0
}

sub days_in_month {
    my ($y, $m) = @_;
    $m = $m +0;
    my %last_day = (
        1 => 31,
        2 => 28,
        3 => 31,
        4 => 30,
        5 => 31,
        6 => 30,
        7 => 31,
        8 => 31,
        9 => 30,
        10=> 31,
        11=> 30,
        12=> 31,
    );
    if (($m ==2) and (is_leap_year($y))) {
        $last_day{2} = 29;
    }
    return $last_day{$m};
}

sub days_to_year_end {
    my ($y, $m, $d) = @_;
    my $days = days_in_month($y,$m) - $d +1 ;
    for my $month ( $m +1 .. 12 ) {
        $days += days_in_month($y, $month);
    }
    return $days;
}

sub months_between {
    my ($syear, $smonth,
        $eyear, $emonth) = @_;
 
    my $months =   (12 - $smonth) 
         + ( ($eyear - $syear -1) * 12 )
         + $emonth + 1;
    return $months;
}
 
sub days_between {
    my ($syear, $smonth, $sday,
        $eyear, $emonth, $eday) = @_;
    # print "start $syear, $smonth, $sday\n end $eyear, $emonth, $eday\n";
    my $days =0;
    if (  sprintf ("%4d%2d%2d", $eyear, $emonth, $eday) 
          lt 
          sprintf("%4d%2d%2d", $syear, $smonth, $sday) ) 
    {
        die "start interval > end interval\n";
    }
    while (    ($syear < $eyear) 
            or ( ($syear == $eyear) and ($smonth < $emonth) ) 
            or ( ($syear == $eyear) and ($smonth == $emonth) and ($sday < $eday) ) 
        ) {
        if ($syear < $eyear) {
            $days += days_to_year_end($syear, $smonth, $sday);
            $syear++;
            $smonth=1;
            $sday=1;
        }
        elsif ($smonth < $emonth) {
            $days += days_in_month($syear, $smonth) - $sday;
            ($syear, $smonth) = next_month($syear, $smonth);
            $sday =1;
        }
        elsif ($sday < $eday) {
            $days += $eday - $sday +1;
            $sday = $eday;
        }
    }
    return $days;
}

sub weeks_between {
    my ($syear, $smonth, $sday,
        $eyear, $emonth, $eday) = @_;
   my $days = days_between ($syear, $smonth, $sday,
            $eyear, $emonth, $eday);
    # print $days, "\n"; exit;
    return int ($days / 7) +1;
}

sub next_month {
    my ($y, $m) = @_;
    if ($m  == 12) {
        $m = 1;
        $y++;
    }
    else {
        $m++;
    }
    return ($y, $m);
}

sub get_help {
    my ($msg) = @_;
    if ($msg) {
        warn "[***] $msg\n\n";
    }

    my $HELP_MSG = q{};
    for my $op ( 
                sort { $parse_options{$a}{so} <=> $parse_options{$b}{so} } 
                grep { $parse_options{$_}{parse}}  keys %parse_options  ) {
        my $param =  $parse_options{$op}{parse};
        my $param_str = q{    };
        my ($short, $long ) = $param =~ / (?: (\w) \| )? (\S+) /x;
        if ($short) {
            $param_str .= q{-} . $short . q{ };
        } 
        $long =~ s/ = s \@? / = name/x;
        $long =~ s/ = i / = number/x;
        $param_str .= q{--} . $long;
        $param_str .= (q{ } x (40 - length($param_str)) );
        my $text_items = $parse_options{$op}{help};
        for my $titem (@{$text_items}) {
            $HELP_MSG .= $param_str . $titem . "\n";
            $param_str = q{ } x 40;
        }
        if (@{$text_items} > 1) {
            $HELP_MSG .= "\n";
        }
        # $HELP_MSG .= "\n";
   }

   print credits(),
          "syntax: $PROGRAM_NAME [options] \n", 
          $HELP_MSG;
    exit( $msg ? 1 : 0 );
}

sub credits {
    my $CREDITS = 
          qq(    The Partition Helper,  version $VERSION\n) 
        . qq(    This program creates a ALTER TABLE statement to add or reorganize\n )
        . qq(    partitions for MySQL 5.1 or later\n)
        . qq(    (C) 2008-2010 Giuseppe Maxia\n);
    return $CREDITS;
}

sub show_options {
    printf "# %-20s %-20s %s\n", 'options', 'default', 'value';
    printf "# %-20s %-20s %s\n", '-' x 20, '-' x 20, '-' x 20;
    for my $op ( sort { $parse_options{$a}{so} <=> $parse_options{$b}{so} }
            keys %parse_options) {
        my $v = $options{$op};
        my $d = $parse_options{$op}{value};
        printf "# %-20s %-20s %s\n", 
                $op, 
                '(' . (defined $d ? $d : '') . ')',  
                defined $v ? $v : '' ;
    }
    print '# ', '-' x 62, "\n";
}

