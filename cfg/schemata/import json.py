import json

with open("cfg/schemata/schemaVersions.schema.json", 'r') as schema_file:
	schema_json = schema_file.read()

schema_dict = json.loads(schema_json)
print(schema_dict)


with open("cfg/test/settings.json", 'r') as settings_file:
	settings_json = settings_file.read()
settings = json.loads(settings_json)