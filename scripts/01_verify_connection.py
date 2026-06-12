"""
01_verify_connection.py

Verify connection to Azure Databricks workspace.
This confirms your credentials are working before we build anything.
"""

import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails

load_dotenv()

def verify_connection():
    """Verify Databricks connection and list workspace info."""
    
    print("="*60)
    print("DATABRICKS CONNECTION TEST")
    print("="*60)
    
    # Check environment variables
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    
    if not host or not token:
        print("\n❌ Missing credentials!")
        print("   Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")
        return False
    
    print(f"\n  Host: {host}")
    print(f"  Token: {'*' * 20}...{token[-4:]}")
    
    try:
        # Initialize client
        client = WorkspaceClient(
            host=host,
            token=token
        )
        
        # Get current user
        me = client.current_user.me()
        print(f"\n✓ Connected as: {me.user_name}")
        
        # List clusters
        print("\n  Existing clusters:")
        clusters = list(client.clusters.list())
        if clusters:
            for c in clusters:
                print(f"    - {c.cluster_name} ({c.state.value})")
        else:
            print("    (none)")
        
        # List warehouses (serverless SQL)
        print("\n  SQL Warehouses:")
        warehouses = list(client.warehouses.list())
        if warehouses:
            for w in warehouses:
                print(f"    - {w.name} ({w.state.value})")
        else:
            print("    (none)")
        
        # Check Unity Catalog
        print("\n  Unity Catalog:")
        try:
            catalogs = list(client.catalogs.list())
            for cat in catalogs:
                print(f"    - {cat.name}")
        except Exception as e:
            print(f"    Not configured yet (expected for new workspace)")
        
        print("\n" + "="*60)
        print("CONNECTION SUCCESSFUL")
        print("="*60)
        print("""
Next steps:
  1. Create a Unity Catalog metastore
  2. Create compute cluster
  3. Set up Delta Lake storage
  4. Build streaming pipeline
""")
        return True
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    verify_connection()