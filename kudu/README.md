# jepsen.kudu

A Clojure library designed to ... well, that part is up to you.

## Usage

FIXME

## Notes on running jepsen.kudu on Docker for Mac

### Memory-related issues
The Knossos take a lot of resources to run, so make sure to provide at least
6GB for the 'jepsen-control' docker container.  Otherwise, OOM killer might
kill corresponding Java process while jepsen is doing analysis of the test
results.

### NTP-related issues
If running jepsen.kudu on Docker for Mac (not Docker Toolbox), make sure
the host MacOS X machine does not synchronize time, otherwise Kudu processes
under test might intermittently crash on 'ntp_gettime()' calls because of
unsynchronized clock.  Error messages look like 'Service Unavaliable:
Error reading clock. Clock considered unsynchronized'.  Apparently,
due to the adjustments of the host clock, xhyve virtual machines are affected:
their clock, which is managed by their own NTP servers, is updated and
for some period of time their clock becomes unsynchronized, so the 'ntptime'
utility returns an error while 'ntpstat' reports "all is well":

root@m1:/# ntpstat
synchronised to local net at stratum 11
   time correct to within 199 ms
   polling server every 64 s
root@m1:/# ntptime
ntp_gettime() returns code 5 (ERROR)
  time dbf32fdd.25137000  Thu, Dec  8 2016  1:03:25.144, (.144828),
  maximum error 69231 us, estimated error 90 us, TAI offset 0
ntp_adjtime() returns code 5 (ERROR)
  modes 0x0 (),
  offset 18.000 us, frequency 2.405 ppm, interval 1 s,
  maximum error 69231 us, estimated error 90 us,
  status 0x40 (UNSYNC),
  time constant 4, precision 1.000 us, tolerance 500 ppm,
root@m1:/#

To disable clock synchronization on MacOS X host machine, do one of the
following:
  * Open 'System Preferences::Date & Time' panel and uncheck the
    'Set data and time automatically' checkbox.
  * In Terminal, run 'sudo systemsetup -setusingnetworktime off'

Once tests are completed, re-enable the MacOS X time synchronization back,
if needed:
  * Open 'System Preferences::Date & Time' panel and check the
    'Set data and time automatically' checkbox.
  * In Terminal, run 'sudo systemsetup -setusingnetworktime on'

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
