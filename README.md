# OT Platform. Additional commands for dataframe. OT plugin.

Additional OTL commands for transforming dataframe in **Dispatcher App**.

For additional information about commands see OTLXTEND_COMMANDS.md

## Dependencies

- dispatcher-sdk_2.11  1.2.0
- sbt 1.5.8
- scala 2.11.12
- eclipse temurin 1.8.0_312 (formerly known as AdoptOpenJDK)

## Deployment

1. make pack or make build.
2. Copy the `build/otlxtend` directory to `/opt/otp/dispatcher/plugins` (or unpack the resulting archive `otlxtend-<PluginVersion>-<Branch>.tar.gz` into the same directory).
3. Rename loglevel.properties.example => loglevel.properties.
4. Rename plugin.conf.example => plugin.conf.
5. If necessary, configure plugin.conf and loglevel.properties.
6. Restart dispatcher.

## Running the tests

sbt test

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the tags on this repository.  

## Authors
 
Sergey Ermilov (sermilov@ot.ru)
Nikolay Ryabykh (nryabykh@isgneuro.com)

## License

[OT.PLATFORM. License agreement.](LICENSE.md)