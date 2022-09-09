# Jumbo for .Net Framework and Mono

Jumbo is a distributed data processing system for Microsoft .Net Framework and Mono. It was created
as a way for me to learn more about MapReduce and [Apache Hadoop](https://hadoop.apache.org/) during
my time as a Ph.D. candidate at [Kitsuregawa Lab](http://www.tkl.iis.u-tokyo.ac.jp/new/?lang=en) at
the University of Tokyo's Institute of Industrial Science.

:warning: **This is not the project you're looking for** :warning:

Or at least, it probably isn't. If you want to play around with Jumbo or learn more about how it
works, look at [JumboCore](https://github.com/SvenGroot/JumboCore) instead, a port of Jumbo to
.Net 6, which is easier to build and run, and has better documentation.

This is the original version of Jumbo, written between 2008 and 2013, targeting Mono 3.0 (for
running on Linux clusters) and the .Net Framework 4.0 (for debugging/testing). Getting it to work
in modern environments probably takes some doing, and I haven't tried it in a long time.

This version of Jumbo is only provided here to preserve the original history of the Jumbo project,
since the JumboCore repository only contains the history of the port to .Net 6.

The only reason to look at it would be to see the differences between the two versions, and maybe
if you're curious about how I handled supporting both .Net Framework and Mono.
