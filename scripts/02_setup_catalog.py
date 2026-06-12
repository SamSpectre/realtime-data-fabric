"""
02_setup_catalog.py

Set up Unity Catalog structure for Project 3.

Unity Catalog hierarchy:
  Catalog (data_ai_mastery_ws)
    └── Schema (github_intelligence)
          ├── bronze_events (raw GitHub events)
          ├── silver_events (cleaned, typed)
          ├── gold_repo_metrics (aggregated repo stats)
          ├── gold_language_trends (language popularity)
          └── gold_developer_activity (contributor patterns)
"""

import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SchemaInfo, VolumeType

load_dotenv()


def setup_catalog():
    """Set up Unity Catalog schema and volumes."""
    
    print("="*60)
    print("UNITY CATALOG SETUP")
    print("="*60)
    
    client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )
    
    # Use the workspace catalog that was auto-created
    catalog_name = "data_ai_mastery_ws"
    schema_name = "github_intelligence"
    
    print(f"\n  Catalog: {catalog_name}")
    print(f"  Schema: {schema_name}")
    
    # Create schema if it doesn't exist
    print(f"\n  Creating schema '{schema_name}'...")
    try:
        schema = client.schemas.create(
            name=schema_name,
            catalog_name=catalog_name,
            comment="GitHub event streaming and intelligence platform"
        )
        print(f"  ✓ Schema created: {catalog_name}.{schema_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  ✓ Schema already exists: {catalog_name}.{schema_name}")
        else:
            print(f"  ✗ Error creating schema: {e}")
            return False
    
    # Create a volume for raw file storage (checkpoints, etc.)
    volume_name = "raw_data"
    print(f"\n  Creating volume '{volume_name}'...")
    try:
        volume = client.volumes.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=volume_name,
            volume_type=VolumeType.MANAGED,
            comment="Raw data and streaming checkpoints"
        )
        print(f"  ✓ Volume created: {catalog_name}.{schema_name}.{volume_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  ✓ Volume already exists")
        else:
            print(f"  ✗ Error creating volume: {e}")
    
    # List what we have
    print("\n  Current schemas in catalog:")
    for s in client.schemas.list(catalog_name=catalog_name):
        print(f"    - {s.name}")
    
    print("\n" + "="*60)
    print("CATALOG SETUP COMPLETE")
    print("="*60)
    print(f"""
Structure created:
  {catalog_name}
    └── {schema_name}
          └── {volume_name} (volume for checkpoints)

Next: Create compute cluster and first notebook.
""")
    
    return True


if __name__ == "__main__":
    setup_catalog()