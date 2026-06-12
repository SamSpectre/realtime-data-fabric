"""
04_setup_secrets.py

Set up Databricks Secrets for secure API key storage.
Run this ONCE from your local machine to create the secret scope and store keys.

Usage:
    uv run python -m project_03_realtime.scripts.04_setup_secrets
"""

import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission

load_dotenv()


def setup_secrets():
    """Create secret scope and add OpenAI API key."""
    
    print("="*60)
    print("DATABRICKS SECRETS SETUP")
    print("="*60)
    
    # Connect to Databricks
    client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )
    
    scope_name = "openai"
    secret_key = "api-key"
    
    # Step 1: Create secret scope
    print(f"\n1. Creating secret scope: {scope_name}")
    try:
        client.secrets.create_scope(scope=scope_name)
        print(f"   ✓ Scope '{scope_name}' created")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ✓ Scope '{scope_name}' already exists")
        else:
            print(f"   ✗ Error: {e}")
            return False
    
    # Step 2: Get OpenAI API key from user
    print(f"\n2. Adding secret: {secret_key}")
    
    # Check if OPENAI_API_KEY is in .env
    openai_key = os.getenv("OPENAI_API_KEY")
    
    if openai_key:
        print("   Found OPENAI_API_KEY in .env file")
        use_env = input("   Use this key? (y/n): ").strip().lower()
        if use_env != 'y':
            openai_key = None
    
    if not openai_key:
        print("   Enter your OpenAI API key (input hidden):")
        import getpass
        openai_key = getpass.getpass("   API Key: ")
    
    if not openai_key or not openai_key.startswith("sk-"):
        print("   ✗ Invalid API key format")
        return False
    
    # Step 3: Store the secret
    try:
        client.secrets.put_secret(
            scope=scope_name,
            key=secret_key,
            string_value=openai_key
        )
        print(f"   ✓ Secret '{secret_key}' stored in scope '{scope_name}'")
    except Exception as e:
        print(f"   ✗ Error storing secret: {e}")
        return False
    
    # Step 4: Verify
    print(f"\n3. Verifying secrets...")
    try:
        secrets = client.secrets.list_secrets(scope=scope_name)
        print(f"   Secrets in '{scope_name}':")
        for secret in secrets:
            print(f"     - {secret.key}")
    except Exception as e:
        print(f"   ✗ Error listing secrets: {e}")
    
    print("\n" + "="*60)
    print("SETUP COMPLETE")
    print("="*60)
    print(f"""
Usage in Databricks notebook:

    # Retrieve API key from Databricks Secrets
    OPENAI_API_KEY = dbutils.secrets.get(scope="openai", key="api-key")
    os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
    
This is the production-standard approach:
  ✓ Secrets encrypted at rest
  ✓ Not visible in notebook code
  ✓ Access controlled by Databricks ACLs
  ✓ Audit logged
""")
    
    return True


if __name__ == "__main__":
    setup_secrets()