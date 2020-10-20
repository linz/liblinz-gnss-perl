#!/usr/bin/perl -pi

BEGIN {
my($s,$m,$h,$dy,$mn,$yr)=localtime();
our $re=sprintf("^%04d\\-%02d\\-%02d \\d\\d\\:\\d\\d\\:\\d\\d",$yr+1900,$mn+1,$dy);
}

s/$re/2020-01-02 03:04:05/;
s~/tmp/\w+/~/tmp/dummy/~g;
