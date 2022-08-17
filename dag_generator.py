from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))

env = Environment(loader=FileSystemLoader(file_dir))

template = env.get_template("templates/emr_jinja_template.py")

with open(f"templates/emr_cfg.yaml", "r") as cfile:
    cfg = yaml.safe_load(cfile)
    with open(f"dags/emr_template_dag.py", "w") as f:
        f.write(template.render(cfg))

