# Racetrack Plugin: Docker Infrastructure

A Racetrack plugin allowing to deploy services to a local Docker

## Setup

1.  Install [racetrack client](https://pypi.org/project/racetrack-client/) and generate ZIP plugin by running:
    ```shell
    make bundle
    ```
    
    Afterward, activate the plugin in Racetrack Dashboard Admin page by uploading the zipped plugin file:
    ```shell
    racetrack plugin install docker-infrastructure-*.zip
    ```
    
    Alternatively, you can install the latest plugin by running:
    ```shell
    racetrack plugin install github.com/TheRacetrack/plugin-docker-infrastructure
    ```
