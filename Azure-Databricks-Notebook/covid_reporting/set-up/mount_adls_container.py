# Databricks notebook source

# COMMAND ----------

def mount_adls(storage_name,container_name):

    #retrieving secrets
    client_id=dbutils.secrets.get(scope='covid-scope',key='covid-client-id')
    client_secret=dbutils.secrets.get(scope='covid-scope',key='covid-client-secret')
    tenant_id=dbutils.secrets.get(scope='covid-scope',key='covid-tenant-id')

    #creating configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

        #unmount mount if exists
    
    if any(mount.mountPoint == f"/mnt/{storage_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_name}/{container_name}")
    else:
        pass
    # creating mounts
    dbutils.fs.mount(
          source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
          mount_point = f"/mnt/{storage_name}/{container_name}",
          extra_configs = configs)
    #display all mounts
    display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls('covid19reportingsamdl','raw')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/covid19reportingsamdl/preprocessed'))

# COMMAND ----------

mount_adls('covid19reportingsamdl','preprocessed')
mount_adls('covid19reportingsamdl','presentation')

# COMMAND ----------

