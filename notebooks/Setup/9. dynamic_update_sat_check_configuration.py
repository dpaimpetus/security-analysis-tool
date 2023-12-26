# Databricks notebook source
import json
import os

# COMMAND ----------

# MAGIC %run ../Utils/initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
# MAGIC
# MAGIC ## SAT Check Configuration
# MAGIC * <i> This utility helps administrators review their current configuration of SAT checks and modify if needed. </i>
# MAGIC * <b> get_all_sat_checks() </b> 
# MAGIC   * Populates 'SAT Check' drop down widget with all relevant checks for the current cloud type & reset all other widgets
# MAGIC   * Administrator chooses a check from the dropdown list
# MAGIC * <b> get_sat_check_config() </b> 
# MAGIC   * Fetches the specific check details from the database for user to view or modify
# MAGIC * <b> set_all_sat_checks() </b> 
# MAGIC   * Updates Database with user changes to check details

# COMMAND ----------

# MAGIC %run ./../Utils/sat_checks_config

# COMMAND ----------

args = dbutils.notebook.entry_point.getCurrentBindings()

# COMMAND ----------

# Parse the string into a list of class objects
dbutils.widgets.text("user_inputs", " ")
parsed_list = json.loads(args['user_inputs'])
user_input = ""
user_enable = 0
user_evaluate = 0
user_alert = 0

# COMMAND ----------

# Define the class to represent the objects
class UserInput:
    def __init__(self, user_input, user_enable, user_evaluate, user_alert):
        self.user_input = user_input
        self.user_enable = user_enable
        self.user_evaluate = user_evaluate
        self.user_alert = user_alert

# COMMAND ----------

#Determine cloud type
def getCloudType(url):
  if '.cloud.' in url:
    return 'aws'
  elif '.azuredatabricks.' in url:
    return 'azure'
  elif '.gcp.' in url:
    return 'gcp'
  return ''

def getConfigPath():
  import os
  cwd = os.getcwd().lower()
  if (cwd.rfind('/includes') != -1) or (cwd.rfind('/setup') != -1) or (cwd.rfind('/utils') != -1):
    return '../../configs'
  elif (cwd.rfind('/notebooks') != -1):
    return '../configs'
  else:
    return 'configs'

# COMMAND ----------

#Reset SAT check widgets 
def get_all_sat_checks():
    #dbutils.widgets.removeAll()

    hostname = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
    cloud_type = getCloudType(hostname)

    s_sql = '''
        SELECT CONCAT_WS('_',check_id, category, check) AS check
        FROM {analysis_schema_name}.security_best_practices 
        WHERE {cloud_type}=1
        '''.format(cloud_type = cloud_type, analysis_schema_name= json_["analysis_schema_name"])

    all_checks = spark.sql(s_sql)
    checks = all_checks.rdd.map(lambda row : row[0]).collect()

    checks.sort()
    #print(checks)
    first_check = str(user_input)
    
    #Define Driver Widgets
    dbutils.widgets.dropdown("sat_check", first_check, [str(x) for x in checks], "a. SAT Check")

# COMMAND ----------

def get_sat_check_config():
    sat_check = dbutils.widgets.get("sat_check")
    check_id = sat_check.split('_')[0]

    s_sql = '''
                SELECT enable, evaluation_value, alert
                FROM {analysis_schema_name}.security_best_practices 
                WHERE check_id= '{check_id}'
            '''.format(check_id = check_id, analysis_schema_name= json_["analysis_schema_name"])

    get_check = spark.sql(s_sql)
    check = get_check.toPandas().to_dict(orient = 'list')
    
#    enable = check['enable'][0]
#    evaluate = check['evaluation_value'][0]
#    alert = check['alert'][0]
    enable = user_enable
    evaluate = user_evaluate
    alert = user_alert


    dbutils.widgets.dropdown("check_enabled", str(enable),  ['0','1'], "b. Check Enabled")
    dbutils.widgets.text("evaluation_value", str(evaluate), "c. Evaluation Value")
    dbutils.widgets.text("alert", str(alert), "d. Alert")

# COMMAND ----------

# Convert the parsed dictionary objects into instances of the UserInput class
list_of_objects = [UserInput(**item) for item in parsed_list]

# Accessing elements in the list of objects (for example, printing user_input)
for obj in list_of_objects:

    user_input = obj.user_input
    user_enable = obj.user_enable
    user_evaluate = obj.user_evaluate
    user_alert = obj.user_alert
    print("---------------------------------------------")
    print(f"{user_input} is {user_enable} is {user_evaluate} is {user_alert}")
    
    get_all_sat_checks()

    get_sat_check_config()

    #Update Database with user changes to check details
    set_sat_check_config()
    


# COMMAND ----------

#reset widgets
get_all_sat_checks()

# COMMAND ----------


