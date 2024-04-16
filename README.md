
<div>
<img src="https://github.com/mage-ai/assets/blob/main/mascots/mascots-shorter.jpeg?raw=true">
</div>

## Data Engineering Zoomcamp project 

Mage is an open-source, hybrid framework for transforming and integrating data. ✨

I utilized the Mage orchestrator to craft the data pipeline for my Data Engineering Zoomcamp project , This repository serves as a component of [Bike-usage-analytics-project](https://github.com/Javeed-Pasha/Bike-usage-analytics-project) repository for my dataenginnering 2024 cohort journey. 
 

## Getting Started
To begin, go to the [Bike-usage-analytics-project](https://github.com/Javeed-Pasha/Bike-usage-analytics-project)  repository, where a Terraform script is employed to set up a GCP VM and install this repository within it.
and follow the steps.
When it initialize this mage repository. It will be present in your project under the name `magic-zoomcamp`. If you changed the varable `PROJECT_NAME` in the `.env` file, it will be named whatever you set it to.

 
This repository should have the following structure:

```
.
├── mage_data
│   └── magic-zoomcamp
├── magic-zoomcamp
│   ├── __pycache__
│   ├── charts
│   ├── custom
│   ├── data_exporters
│   ├── data_loaders
│   ├── dbt
│   ├── extensions
│   ├── interactions
│   ├── pipelines
│   ├── scratchpads
│   ├── transformers
│   ├── utils
│   ├── __init__.py
│   ├── io_config.yaml
│   ├── metadata.yaml
│   └── requirements.txt
├── Dockerfile
├── README.md
├── dev.env
├── docker-compose.yml
└── requirements.txt
```

