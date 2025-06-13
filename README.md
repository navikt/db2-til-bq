# venteregister-data

Dette skal være en jobb som henter data fra stormaskin og legger på bigquery

## Local Development

### Build

1. Build Docker image:

    ```shell
    docker build . -t venteregister-data
    ```

2. Run Docker image:

    ```shell
    docker run -p 8080:8080 venteregister-data
    ```
