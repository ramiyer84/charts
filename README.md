version: '3.7'

services:
  dms-migration-db:
    container_name: dms-migration-db
    image: clpregistrystaging.al.intraxa/dms-migration-db:1.0.0.200
    restart: unless-stopped
    ports:
      - "5432:5432"
#  dms-db:
#    container_name: dms-db
#    image: clpregistrystaging.al.intraxa/dms-test-database:phase2-migration
#    restart: unless-stopped
#    environment:
#      - ACCEPT_EULA=Y
#      - SA_PASSWORD=Limerick$$01
#    ports:
#      - "1433:1433"
  rabbitmq:
    image: rabbitmq:3.12.4-management-alpine
    restart: unless-stopped
    ports:
      - "5672:5672"
      - "15672:15672"
  dms-migration-service:
    image: dms-migration-service
    restart: unless-stopped
    environment:
      - JAVA_OPTS=-Dhttp.proxyHost=uaclplgapp23.al.intraxa -Dhttp.proxyPort=5300 -Dhttp.nonProxyHosts="localhost|*.al.intraxa|cmx-eu-uat.corp.intraxa" -Dhttps.proxyHost=uaclplgapp23.al.intraxa -Dhttps.proxyPort=5300 -Dhttps.nonProxyHosts="localhost|*.al.intraxa|cmx-eu-uat.corp.intraxa"
      - logging_level_org_springframework_security=TRACE
      - logging_level_axa_partners=TRACE
      - server_port=8080
      - logging_level_org_springframework_web_filter_CommonsRequestLoggingFilter=TRACE
      - cache_sf_persistentDirectory=temp
      - cron.decisionsProcessingJob=0 * * * * *
      - cron.macaoNotesMigrationJob=0 * * * * *
      - cron_macaoDecisionsMigrationJob=0 * * * * *
      - databaseName=DMSRISKS
      - decisions_url=jdbc:sqlserver://localhost:1433\;databaseName=DMSDECISIONS\;Persist Security Info=False\;MultipleActiveResultSets=False\;Encrypt=True\;TrustServerCertificate=True\;Connection Timeout=30
      - dms_processingBatchSize=2000
      - encryption_secret=11TVvCmwGTHWitgvhLHAozV1CnZhieSa2vvBEzWtBac=
      - dms_processingThreads=1
      - dms_updatePackageProcessingBatchSize=2000
      - dms_updatePackageThreads=1
      - spring_rabbitmq_host=rabbitmq
      - spring_rabbitmq_password=Password1
      - rabbitmq_prefix=axa.partners.clp.dms-migration-service-pp.
      - spring_rabbitmq_username=dms-migration
      - spring_rabbitmq_virtual_host=dms-migration
      - risks_url=jdbc:sqlserver://localhost:1433\;databaseName=DMSRISKS\;Persist Security Info=False\;MultipleActiveResultSets=False\;Encrypt=True\;TrustServerCertificate=True\;Connection Timeout=30
      - selmed_dms_page=10000
      - sf_batchSize=1000
      - sf_maxThreads=3
      - jdbc_url=jdbc:postgresql://dms-migration-db/migrationdb
    ports:
      - "5005:5005"
      - "8080:8080"
  dms-migration-service-ui:
    image: dms-migration-service-ui
    restart: unless-stopped
    environment:
      - VUE_APP_DMS_MIGRATION_SERVICE_URL=http://localhost:8080
      - VUE_APP_DMS_MIGRATION_SERVICE_UI_URL=http://localhost
      - VUE_APP_DMS_MIGRATION_SERVICE_CLIENT_ID=JKi65vqF3fLNjWqT6ubPOKr6vbkMnwmX
      - VUE_APP_DMS_MIGRATION_SERVICE_AUDIENCE=https://dms-migration.dev.al.intraxa
      - VUE_APP_DMS_MIGRATION_SERVICE_OIDC_LOGOUT_URL=http://localhost/logout
      - VUE_APP_DMS_MIGRATION_SERVICE_OIDC_CONFIGURATION_URL=https://axa-clp-dp-generic-apis-dev.eu.auth0.com/.well-known/openid-configuration
      - UI_SECRET_KEY=0HS4Dibv/LBdIU7AiNTSr/LiVkX2dA2iR8vNUO3PSyg=
    ports:
      - "80:80"
networks:
  default:
    external: false
