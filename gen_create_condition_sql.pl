#!/usr/bin/perl

use Getopt::Long;

my $table_name;
my $part_name;
my $comment;
my $condition_name;
my $table_shortname;
my @column;
my @colum_type;

$argc = length(@ARGV);

sub help {
    print "\n";
    print "Usage: $0 --name [table_name] --part [part_name] --comment [comment] <options>\n";
    print "       Generates the SQL commands to create the table [table_name].\n";
    print "       [table_name]: the name of the table to create\n";
    print "       [part_name]:  the name of the part to which the condition applies\n";
    print "       [comment]: a description for this condition\n";
    print "\n";
    print "--condition condition_name: is the name of the condition to generate\n";
    print "            (by default it is the table_name with underscores substituted \n";
    print "            by blanks)";
    print "--shortname short_name: provides a shorter version of\n";
    print "            the name of the table. Mandatory if the name\n";
    print "            is longer than 40 characters.\n";
    print "--column    column_name: column name of data to be stored in the table.\n";
    print "            If the column can be NULL, prepend a @ to its name.\n";
    print "--type      column_type: the SQL type of the column in the table.\n";
    print "\nOptions --column and --type can be repeated as many times\n";
    print "as needed. If type contains ( or ), the type must be given in quotes.\n";
}

GetOptions("name=s"      => \$table_name,
	   "part=s"      => \$part_name,
	   "condition=s" => \$condition_name,
	   "column=s"    => \@column,
	   "type=s"      => \@column_type,
           "comment=s"   => \$comment);

if (length($table_name) <= 0) {
    print "\033[5;31;47mERR\033[0m: table name not given\n";
    help();
    exit(-1);
}

$table_shortname = $table_name;
$table_shortname =~ s/[A,E,I,O,U,a,e,i,o,u]//g;
if (length($part_name) == 0) {
    print "\033[5;31;47mERR\033[0m: no part name given\n";
    help();
    exit(-1);
}

if (length($comment) == 0) {
    print "\033[5;31;47mERR\033[0m: no comment given\n";
    help();
    exit(-1);
}

if (length($condition_name) == 0) {
    $condition_name = $table_name;
    $condition_name =~ s/_/ /g;
}

if (length($table_shortname) > 40) {
    print "\033[5;31;47mERR\033[0m: table short name is too long\n";
    help();
    exit(-2);
}

if (length(@column) < 1) {
    print "\033[5;31;47mERR\033[0m: you should provide at least one column\n";
    help();
    exit(-3);
}

$n = @column;
$nt = @column_type;

if ($n != $nt) {
    print "\033[5;31;47mERR\033[0m: the number of columns and their types do not match\n";
    help();
    exit(-4);
}

print("\n\n\n\/*\n        To be run as CMS_MTD_CORE_COND\n*\/\n\n" .
      "INSERT INTO CMS_MTD_CORE_COND.KINDS_OF_CONDITIONS " .
      "(NAME, IS_RECORD_DELETED, EXTENSION_TABLE_NAME, COMMENT_DESCRIPTION) " .
      "VALUES ('$condition_name', 'F', '$table_name', '$comment');\n"
    );
      
$cmd = "cat btl_conditions_template.sql | sed -e 's/TEMPLATE_NAME/$table_name/g' |" .
    " sed -e 's/TEMPLATE_SHRTNAME/$table_shortname/g' |" .
    " sed -e 's/TEMPLATE_PART_NAME/$part_name/g' |" .
    " sed -e 's/TEMPLATE_CONDITION_NAME/$condition_name/g'" .
    " > gen_create_condition_sql.sql";

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

