from flask import Blueprint, render_template_string
from airflow.plugins_manager import AirflowPlugin


custom_menu_bp = Blueprint(
    "custom_menu_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    url_prefix="/custom_menu"
)


@custom_menu_bp.route("/", methods=["GET"])
def custom_dashboard():
    """Custom dashboard page"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Custom Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .header { color: #1f77b4; border-bottom: 2px solid #1f77b4; padding-bottom: 10px; }
            .content { margin-top: 20px; }
            .card { background: #f9f9f9; padding: 20px; margin: 10px 0; border-radius: 5px; }
        </style>
    </head>
    <body>
        <h1 class="header">Custom Dashboard</h1>
        <div class="content">
            <div class="card">
                <h3>Welcome to your custom dashboard!</h3>
                <p>This is a custom page added via Airflow plugin.</p>
            </div>
            <div class="card">
                <h3>Quick Stats</h3>
                <p>This could show your custom metrics or data.</p>
            </div>
        </div>
    </body>
    </html>
    """
    return render_template_string(html_content)


#@custom_menu_bp.route("/analytics", methods=["GET"])
#def custom_analytics():
#    """Custom analytics page"""
#    html_content = """
#    <!DOCTYPE html>
#    <html>
#    <head>
#        <title>Custom Analytics</title>
#        <style>
#            body { font-family: Arial, sans-serif; margin: 40px; }
#            .header { color: #ff7f0e; border-bottom: 2px solid #ff7f0e; padding-bottom: 10px; }
#            .content { margin-top: 20px; }
#            .card { background: #f9f9f9; padding: 20px; margin: 10px 0; border-radius: 5px; }
#        </style>
#    </head>
#    <body>
#        <h1 class="header">Custom Analytics</h1>
#        <div class="content">
#            <div class="card">
#                <h3>Analytics Dashboard</h3>
#                <p>Your custom analytics and reporting would go here.</p>
#            </div>
#            <div class="card">
#                <h3>Charts and Graphs</h3>
#                <p>This is where you could add custom visualizations.</p>
#            </div>
#        </div>
#    </body>
#    </html>
#    """
#    return render_template_string(html_content)


class CustomMenuPlugin(AirflowPlugin):
    name = "custom_menu_plugin"
    flask_blueprints = [custom_menu_bp]

    appbuilder_menu_items = [
        {
            "name": "Custom Dashboard",
            "href": "/custom_menu/",
            "category": "AiiDA Database Viewer"
        },
        #{
        #    "name": "Custom Analytics",
        #    "href": "/custom_menu/analytics",
        #    "category": "Custom Tools"
        #}
    ]
