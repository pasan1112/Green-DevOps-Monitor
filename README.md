# Green DevOps Monitor

A sustainability-aware CI/CD monitoring component for measuring stage-level pipeline metrics and estimating energy and carbon emissions.

## Features

- Jenkins stage-level monitoring
- CPU and memory usage collection
- Total and active energy estimation
- Carbon emission estimation using ElectricityMaps
- Structured CSV sustainability dataset
- Sample Flask backend application
- Dashboard support

## Pipeline Stages

- Build: installs backend dependencies
- Test: runs backend unit tests
- Deploy: builds and runs Docker container

## Monitoring Output

Metrics are stored in:

```text
data/metrics.csv