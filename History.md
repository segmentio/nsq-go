
v1.2.7 / 2022-05-26
===================

* fix write Command deadlock

v1.2.6 / 2022-05-24
===================

  * Added drainAndJoinAwait for consumer stop to handle deadlock scenario related to NSQ message timeout

v1.2.5 / 2021-10-20
===================

  * Optionally return connection errors for producer requests
  * Add producer StopWithDrain

v1.2.4 / 2020-07-13
===================

  * Deprecate segment/nsq Docker image
  * Remove dependency on github.com/segmentio/services

v1.2.3 / 2019-10-29
===================

  * Drain in-flight messages on close

v1.2.2 / 2018-11-01
===================

  * Added support for setting msg_timeout in Identify command
