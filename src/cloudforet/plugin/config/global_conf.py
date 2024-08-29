# ICON URL
ASSET_URL = "https://spaceone-custom-assets.s3.ap-northeast-2.amazonaws.com/console-assets/icons/cloud-services/google_cloud"

RECOMMENDATION_TYPE_DOCS_URL = "https://cloud.google.com/recommender/docs/recommenders"

UNAVAILABLE_RECOMMENDER_IDS = [
    "google.cloudbilling.commitment.SpendBasedCommitmentRecommender",
    "google.accounts.security.SecurityKeyRecommender",
    "google.cloudfunctions.PerformanceRecommender",
]

REGION_INFO = {
    "asia-east1": {
        "name": "Taiwan (Changhua County)",
        "tags": {
            "latitude": "24.051196",
            "longitude": "120.516430",
            "continent": "asia_pacific",
        },
    },
    "asia-east2": {
        "name": "Hong Kong",
        "tags": {
            "latitude": "22.283289",
            "longitude": "114.155851",
            "continent": "asia_pacific",
        },
    },
    "asia-northeast1": {
        "name": "Japan (Tokyo)",
        "tags": {
            "latitude": "35.628391",
            "longitude": "139.417634",
            "continent": "asia_pacific",
        },
    },
    "asia-northeast2": {
        "name": "Japan (Osaka)",
        "tags": {
            "latitude": "34.705403",
            "longitude": "135.490119",
            "continent": "asia_pacific",
        },
    },
    "asia-northeast3": {
        "name": "South Korea (Seoul)",
        "tags": {
            "latitude": "37.499968",
            "longitude": "127.036376",
            "continent": "asia_pacific",
        },
    },
    "asia-south1": {
        "name": "India (Mumbai)",
        "tags": {
            "latitude": "19.164951",
            "longitude": "72.851765",
            "continent": "asia_pacific",
        },
    },
    "asia-south2": {
        "name": "India (Delhi)",
        "tags": {
            "latitude": "28.644800",
            "longitude": "77.216721",
            "continent": "asia_pacific",
        },
    },
    "asia-southeast1": {
        "name": "Singapore (Jurong West)",
        "tags": {
            "latitude": "1.351376",
            "longitude": "103.709574",
            "continent": "asia_pacific",
        },
    },
    "asia-southeast2": {
        "name": "Indonesia (Jakarta)",
        "tags": {
            "latitude": "-6.227851",
            "longitude": "106.808169",
            "continent": "asia_pacific",
        },
    },
    "australia-southeast1": {
        "name": "Australia (Sydney)",
        "tags": {
            "latitude": "-33.733694",
            "longitude": "150.969840",
            "continent": "asia_pacific",
        },
    },
    "australia-southeast2": {
        "name": "Australia (Melbourne)",
        "tags": {
            "latitude": "-37.840935",
            "longitude": "144.946457",
            "continent": "asia_pacific",
        },
    },
    "europe-north1": {
        "name": "Finland (Hamina)",
        "tags": {
            "latitude": "60.539504",
            "longitude": "27.113819",
            "continent": "europe",
        },
    },
    "europe-west1": {
        "name": "Belgium (St.Ghislain)",
        "tags": {
            "latitude": "50.471248",
            "longitude": "3.825493",
            "continent": "europe",
        },
    },
    "europe-west2": {
        "name": "England, UK (London)",
        "tags": {
            "latitude": "51.515998",
            "longitude": "-0.126918",
            "continent": "europe",
        },
    },
    "europe-west3": {
        "name": "Germany (Frankfurt)",
        "tags": {
            "latitude": "50.115963",
            "longitude": "8.669625",
            "continent": "europe",
        },
    },
    "europe-west4": {
        "name": "Netherlands (Eemshaven)",
        "tags": {
            "latitude": "53.427625",
            "longitude": "6.865703",
            "continent": "europe",
        },
    },
    "europe-west6": {
        "name": "Switzerland (Zürich)",
        "tags": {
            "latitude": "47.365663",
            "longitude": "8.524881",
            "continent": "europe",
        },
    },
    "northamerica-northeast1": {
        "name": "Canada, Québec (Montréal)",
        "tags": {
            "latitude": "45.501926",
            "longitude": "-73.570086",
            "continent": "north_america",
        },
    },
    "northamerica-northeast2": {
        "name": "Canada, Ontario (Toronto)",
        "tags": {
            "latitude": "50.000000",
            "longitude": "-85.000000",
            "continent": "north_america",
        },
    },
    "southamerica-east1": {
        "name": "Brazil, São Paulo (Osasco)",
        "tags": {
            "latitude": "43.8345",
            "longitude": "2.1972",
            "continent": "south_america",
        },
    },
    "southamerica-west1": {
        "name": "Chile (Santiago)",
        "tags": {
            "latitude": "-33.447487",
            "longitude": "-70.673676",
            "continent": "south_america",
        },
    },
    "us-central1": {
        "name": "US, Iowa (Council Bluffs)",
        "tags": {
            "latitude": "41.221419",
            "longitude": "-95.862676",
            "continent": "north_america",
        },
    },
    "us-east1": {
        "name": "US, South Carolina (Moncks Corner)",
        "tags": {
            "latitude": "33.203394",
            "longitude": "-79.986329",
            "continent": "north_america",
        },
    },
    "us-east4": {
        "name": "US, Northern Virginia (Ashburn)",
        "tags": {
            "latitude": "39.021075",
            "longitude": "-77.463569",
            "continent": "north_america",
        },
    },
    "us-west1": {
        "name": "US, Oregon (The Dalles)",
        "tags": {
            "latitude": "45.631800",
            "longitude": "-121.200921",
            "continent": "north_america",
        },
    },
    "us-west2": {
        "name": "US, California (Los Angeles)",
        "tags": {
            "latitude": "34.049329",
            "longitude": "-118.255265",
            "continent": "north_america",
        },
    },
    "us-west3": {
        "name": "US, Utah (Salt Lake City)",
        "tags": {
            "latitude": "40.730109",
            "longitude": "-111.951386",
            "continent": "north_america",
        },
    },
    "us-west4": {
        "name": "US, Nevada (Las Vegas)",
        "tags": {
            "latitude": "36.092498",
            "longitude": "-115.086073",
            "continent": "north_america",
        },
    },
    "global": {"name": "Global"},
}

# Recommendation Map by Crawling Google Cloud Console
RECOMMENDATION_MAP = {
    "google.bigquery.capacityCommitments.Recommender": {
        "category": "Cost",
        "name": "BigQuery edition slot recommender",
        "shortDescription": "Optimize BigQuery spend with slot commitments",
    },
    "google.bigquery.table.PartitionClusterRecommender": {
        "category": "Cost",
        "name": "BigQuery partitioning and clustering recommender",
        "shortDescription": "Partition or cluster your tables",
    },
    "google.run.service.CostRecommender": {
        "category": "Cost",
        "name": "Cloud Run CPU allocation recommender",
        "shortDescription": "Switch to CPU always allocated",
    },
    "google.cloudsql.instance.IdleRecommender": {
        "category": "Cost",
        "name": "Cloud SQL idle instance recommender",
        "shortDescription": "Remove unused SQL instances",
    },
    "google.cloudsql.instance.OverprovisionedRecommender": {
        "category": "Cost",
        "name": "Cloud SQL overprovisioned instance recommender",
        "shortDescription": "Resize overprovisioned SQL instances",
    },
    "google.compute.commitment.UsageCommitmentRecommender": {
        "category": "Cost",
        "name": "Committed use discount recommender",
        "shortDescription": "Reduce costs through commitments",
    },
    "google.compute.image.IdleResourceRecommender": {
        "category": "Cost",
        "name": "Idle custom image recommender",
        "shortDescription": "Remove unused images",
    },
    "google.container.DiagnosisRecommender": {
        "category": "Reliability",
        "name": "GKE diagnosis recommender",
        "shortDescription": "Mitigate risks, troubleshoot and optimize your usage of GKE",
    },
    "google.compute.address.IdleResourceRecommender": {
        "category": "Cost",
        "name": "Idle IP address recommender",
        "shortDescription": "Remove unused IPs",
    },
    "google.compute.disk.IdleResourceRecommender": {
        "category": "Cost",
        "name": "Idle persistent disk recommender",
        "shortDescription": "Backup and remove unused disks",
    },
    "google.compute.instance.IdleResourceRecommender": {
        "category": "Cost",
        "name": "Idle VM recommender",
        "shortDescription": "Remove unused VMs",
    },
    "google.iam.policy.Recommender": {
        "category": "Security",
        "name": "IAM recommender",
        "shortDescription": "Remove excess permissions",
    },
    "google.run.service.IdentityRecommender": {
        "category": "Security",
        "name": "Cloud Run Service Security recommenders",
        "shortDescription": "Increase Cloud Run Service security",
    },
    "google.run.service.SecurityRecommender": {
        "category": "Security",
        "name": "Cloud Run Service Security recommenders",
        "shortDescription": "Increase Cloud Run Service security",
    },
    "google.resourcemanager.projectUtilization.Recommender": {
        "category": "Security",
        "name": "Unattended project recommender",
        "shortDescription": "Reclaim or remove unused projects",
    },
    "google.cloud.security.GeneralRecommender": {
        "category": "Security",
        "name": "Advisory Notifications recommender",
        "shortDescription": "Ensure that the right parties within your organization have access to view critical security and privacy notifications",
    },
    "google.compute.instanceGroupManager.MachineTypeRecommender": {
        "category": "Performance",
        "name": "Managed instance group machine type recommender",
        "shortDescription": "Resize MIG machine types",
    },
    "google.compute.instance.MachineTypeRecommender": {
        "category": "Performance",
        "name": "VM machine type recommender",
        "shortDescription": "Resize VM machine types",
    },
    "google.cloudsql.instance.PerformanceRecommender": {
        "category": "Performance",
        "name": "Cloud SQL performance recommender",
        "shortDescription": "Improve Cloud SQL instance performance",
    },
    "google.cloudsql.instance.UnderprovisionedRecommender": {
        "category": "Performance",
        "name": "Cloud SQL underprovisioned recommender",
        "shortDescription": "Optimize underprovisioned Cloud SQL instances",
    },
    "google.resourcemanager.serviceLimit.Recommender": {
        "category": "Reliability",
        "name": "Service limit (quota) recommender",
        "shortDescription": "Manage service and quota usage to avoid hitting limits",
    },
    "google.cloudsql.instance.ReliabilityRecommender": {
        "category": "Reliability",
        "name": "Cloud SQL Reliability recommender",
        "shortDescription": "Enable High Availability on Cloud SQL Instances",
    },
    "google.cloudsql.instance.OutOfDiskRecommender": {
        "category": "Reliability",
        "name": "Cloud SQL out-of-disk recommender",
        "shortDescription": "Prevent CloudSQL out-of-disk outage",
    },
    "google.resourcemanager.project.ChangeRiskRecommender": {
        "category": "Reliability",
        "name": "Change Risk Recommendations",
        "shortDescription": "Reduce risk of misconfigurations",
    },
    "google.iam.serviceAccount.ChangeRiskRecommender": {
        "category": "Reliability",
        "name": "Change Risk Recommendations",
        "shortDescription": "Reduce risk of misconfigurations",
    },
    "google.iam.policy.ChangeRiskRecommender": {
        "category": "Reliability",
        "name": "Change Risk Recommendations",
        "shortDescription": "Reduce risk of misconfigurations",
    },
    "google.cloud.deprecation.GeneralRecommender": {
        "category": "Manageability",
        "name": "Deprecation and Breaking Changes recommender",
        "shortDescription": "Prevent service interruptions due to deprecations and breaking changes",
    },
    "google.clouderrorreporting.Recommender": {
        "category": "Manageability",
        "name": "Error Reporting notification recommender",
        "shortDescription": "Receive notifications for errors in your project",
    },
    "google.gmp.project.ManagementRecommender": {
        "category": "Manageability",
        "name": "Google Maps Platform project management recommender",
        "shortDescription": "Restrict your API keys",
    },
    "google.logging.productSuggestion.ContainerRecommender": {
        "category": "Manageability",
        "name": "Product suggestion recommender",
        "shortDescription": "Explore more helpful products",
    },
}
