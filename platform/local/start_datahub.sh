# python3 -m pip install --upgrade pip wheel setuptools
# python3 -m pip install --upgrade acryl-datahub
# datahub version

datahub docker quickstart --arch m1
# http://localhost:9002/

# username: datahub
# password: datahub

# https://datahubproject.io/docs/quickstart/

# stop: datahub docker quickstart --stop
# reset: datahub docker nuke
# upgrade datahub: datahub docker quickstart

# connect mongodb
##############################
# source:
#     type: mongodb
#     config:
#         # Coordinates
#         connect_uri: localhost:27017 # Your MongoDB connect URI, e.g. "mongodb://localhost"

#         # Credentials
#         # Add secret in Secrets Tab with relevant names for each variable
#         # username: "${MONGO_USERNAME}" # Your MongoDB username, e.g. admin
#         # password: "${MONGO_PASSWORD}" # Your MongoDB password, e.g. password_01

#         # Options (recommended)
#         enableSchemaInference: True
#         useRandomSampling: True
#         maxSchemaSize: 300
##############################

