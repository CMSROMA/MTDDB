#!/usr/bin/perl

use Getopt::Long;

my $table_name;
my $table_shortname;
my @column;
my @colum_type;

$argc = length(@ARGV);

sub help {
    print "Usage: $0 --name [table_name] <options>\n";
    print "       Generates the SQL commands to create the table [table_name].\n";
    print "       [table_name]: the name of the table to create\n";
    print "\n";
    print "--shortname short_name: provides a shorter version of\n";
    print "            the name of the table. Mandatory if the name\n";
    print "            is longer than 25 characters.\n";
    print "--column    column_name: the name of a data column in the table.\n";
    print "            if the columns can be NULL, prepend a @ to its name.\n";
    print "--type      column_type: the SQL type of the column in the table.\n";
    print "\nOptions --column and --type can be repeated as many times\n";
    print "as needed. If type contains ( or ), the type must be given in quotes.\n";
}

GetOptions("name=s"      => \$table_name,
	   "shortname=s" => \$table_shortname,
	   "column=s"    => \@column,
	   "type=s"      => \@column_type);

if (length($table_name) <= 0) {
    print "ERR: table name not given\n";
    help();
    exit(-1);
}

if (length($table_shortname) == 0) {
    $table_shortname = $table_name;
} 

if (length($table_shortname) > 25) {
    print "ERR: table short name is too long\n";
    help();
    exit(-2);
}

if (length(@column) < 1) {
    print "ERR: you should provide at least one column\n";
    help();
    exit(-3);
}

$n = @column;
$nt = @column_type;

if ($n != $nt) {
    print "ERR: the number of columns and their types do not match\n";
    help();
    exit(-4);
}

$cmd = "cat btl_conditions_template.sql | sed -e 's/TEMPLATE_NAME/$table_name/g' |" .
    " sed -e 's/TEMPLATE_SHRTNAME/$table_shortname/g' > gen_create_condition_sql.sql";

`$cmd`;

open IN, 'gen_create_condition_sql.sql';
@buffer = <IN>;
close IN;

sub insertCols {
    my $n = $_[0];
    my $cref = $_[1];
    my $tref = $_[2];
    my @column = @$cref;
    my @column_type = @$tref;
    for (my $i = 0; $i < $n; $i++) {
	my $col = $column[$i];
	$opt = "NOT NULL";
	if ($col =~ m/^@/) {
	    $col =~ s/^@//;
	    $opt = "";
	}
	my $typ = $column_type[$i];
	print "  $col\t$typ\t$opt";
	if ($i < $n - 1) {
	    print ",";
	}
	print "\n";
    }
}

foreach $l (@buffer) {
    if ($l !~ m/TEMPLATE_COLUMNS/) {
	print $l;
    } else {
	insertCols($n, \@column, \@column_type);
    }
}

unlink 'gen_create_condition_sql.sql';

