use strict;
use warnings;

package LINZ::RunBatch;


=head1 LINZ::RunBatch Run a script using \"at\"

Tests for -b or -B logfile from the beginning of @ARGV, and if found
restarts the perl script with an "at now" command.  This disconnects 
the script from the current shell (eg so that it continue running
in the background.

If the -b or -B is not the first argument then this has no effect.

    use RunBatch;

=cut

if( @ARGV && ($ARGV[0] eq '-b' || $ARGV[0] eq '-B') )
{
    my $batch=0;
    my $batchlog=0;

    if( $ARGV[0] eq '-b' )
    {
        $batch=1;
        shift(@ARGV);
        my($sc,$mi,$hr,$dy,$mo,$yr)=(localtime())[0,1,2,3,4,5];
        my $script=$0;
        $script=~ s/.*[\\\/]//;
        $script=~ s/\..*//;
        $batchlog=sprintf("%s_%04d%02d%02d%02d%02d%02d.log",
            $script,$yr+1900,$mo+1,$dy,$hr,$mi,$sc);
    }

    elsif( $ARGV[0] eq '-B' )
    {
        $batch=1;
        shift(@ARGV);
        $batchlog=shift(@ARGV);
        die "Log file not specified with -B\n" if ! $batchlog;
    }

    if( $batch )
    {
        my $cmd=$^X;
        unshift(@ARGV,$0);
        foreach my $a (@ARGV)
        {
            if( $a !~ /^[\w\/\.]+$/ )
            {
                $a =~ s/\'/'"'"'/g;
                $a = "'".$a."'";
            }
            $cmd .= ' '.$a;
        }
        print "Running in batch mode\n";
        print "Command: $cmd\n";
        print "Log file: $batchlog\n";

        $cmd .=' > "'.$batchlog.'" 2>&1';
        open(my $f,"| at now") || die "Cannot start \"at\" command.\n";
        print $f $cmd;
        close($f);
        exit();
    }
}

1;
