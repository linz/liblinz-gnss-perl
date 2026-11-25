use strict;

package LINZ::GNSS::TransferError;

=head1 LINZ::GNSS::TransferError

An error retrieving data from a data center, thrown (with die) by data center
classes so that the caller can tell whether it is worth retrying the request, and
how long the server asked it to wait.

The object stringifies to its message, so code that treats errors as strings
(for example logging $@) works unchanged.

  LINZ::GNSS::TransferError->throw(
      message=>"Cannot retrieve $url: Service Unavailable\n",
      code=>503, retryable=>1, retry_after=>30 );

  if( LINZ::GNSS::TransferError::IsRetryable($@) ) { ... }

=cut

use Scalar::Util qw(blessed);
use HTTP::Date qw(str2time);
use overload '""' => sub { $_[0]->{message} }, fallback => 1;

# HTTP status codes worth retrying: request timeout, too early, too many requests,
# and server errors that are usually transient.  (LWP also reports connection
# failures and timeouts as 500 responses.)

our %RetryableHttpCodes=map { $_ => 1 } (408, 425, 429, 500, 502, 503, 504);

# FTP reply codes worth retrying: service not available, cannot open data connection,
# connection closed / transfer aborted, local error in processing

our %RetryableFtpCodes=map { $_ => 1 } (421, 425, 426, 451);

=head2 LINZ::GNSS::TransferError->new(%args)

Arguments are message (ending with a newline), code (HTTP or FTP status code if
there is one), retryable (true if the request may succeed if retried), and
retry_after (seconds the server asked the client to wait, if it did).

=cut

sub new
{
    my($class,%args)=@_;
    my $message=$args{message} // 'Transfer failed';
    $message .= "\n" if $message !~ /\n$/;
    my $self={
        message=>$message,
        code=>$args{code},
        retryable=>$args{retryable} ? 1 : 0,
        retry_after=>$args{retry_after},
        };
    return bless $self, $class;
}

sub throw
{
    my($class,%args)=@_;
    die $class->new(%args);
}

sub message { return $_[0]->{message}; }
sub code { return $_[0]->{code}; }
sub retryable { return $_[0]->{retryable}; }
sub retry_after { return $_[0]->{retry_after}; }

=head2 $error->throttled

True if the server said it was overloaded or rate limiting the client (HTTP 429 or 503).

=cut

sub throttled
{
    my($self)=@_;
    my $code=$self->{code} // '';
    return $code eq '429' || $code eq '503';
}

=head2 LINZ::GNSS::TransferError->FromHttpResponse($response,$message)

Creates an error from a failed HTTP::Response.  $message is the start of the
message, to which the response status message is added.

=cut

sub FromHttpResponse
{
    my($class,$response,$message)=@_;
    my $code=$response->code;
    my $retry_after;
    my $header=$response->header('Retry-After');
    if( defined($header) )
    {
        $header =~ s/^\s+|\s+$//g;
        if( $header =~ /^\d+$/ )
        {
            $retry_after=$header+0;
        }
        else
        {
            my $time=str2time($header);
            if( defined($time) )
            {
                $retry_after=$time-time();
                $retry_after=0 if $retry_after < 0;
            }
        }
    }
    return $class->new(
        message=>"$message: ".$response->message."\n",
        code=>$code,
        retryable=>$RetryableHttpCodes{$code} ? 1 : 0,
        retry_after=>$retry_after,
        );
}

=head2 LINZ::GNSS::TransferError::IsRetryableFtpCode($code)

True if an FTP reply code is worth retrying.  An empty or 0 code (no reply, for
example a dropped connection or timeout) is treated as retryable.

=cut

sub IsRetryableFtpCode
{
    my($code)=@_;
    return 1 if ! $code;
    return $RetryableFtpCodes{$code} ? 1 : 0;
}

=head2 LINZ::GNSS::TransferError::IsRetryable($error)

True if $error (usually $@) is a LINZ::GNSS::TransferError that is worth retrying.

=cut

sub IsRetryable
{
    my($error)=@_;
    return blessed($error) && $error->isa('LINZ::GNSS::TransferError') && $error->retryable;
}

1;
