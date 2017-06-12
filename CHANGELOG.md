This file lists only breaking changes


2.0.0
-----

  * The package `pynesis.backends` has been renamed to `pynesis.streams`
  * All *Backend related classes have been renamed to * stream:
    `Backend` -> `Stream`,
    `KinesisBackend` -> `KinesisStream`,
    `DummyBackend` -> `DummyStream`
  * `*Stream.put()` keyword argument `record` has been renamed to `data`
