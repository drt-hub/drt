"""Email destination — SMTP.
 
Sends one e-mail per record via Python's built-in :mod:`smtplib`.
Supports Jinja2 templates for both the subject line and the body.
No third-party dependencies beyond those already used by *drt*
(Jinja2 is already required for template rendering).
 
Credentials can be supplied directly in the config dataclass **or** via
environment-variable names so that secrets stay out of committed YAML.
 
Example sync YAML (plain text, STARTTLS):
 
    destination:
      type: email_smtp
      smtp_host: smtp.gmail.com
      smtp_port: 587
      use_tls: true
      username_env: SMTP_USER
      password_env: SMTP_PASSWORD
      from_address: alerts@example.com
      to_addresses:
        - oncall@example.com
      subject_template: "Alert: {{ row.title }}"
      body_template: |
        Hello,
 
        A new alert was triggered for {{ row.name }}.
 
        Details: {{ row.description }}
        HTML body example:
 
    destination:
      type: email_smtp
      smtp_host: smtp.sendgrid.net
      smtp_port: 465
      use_ssl: true
      username_env: SMTP_USER
      password_env: SMTP_PASSWORD
      from_address: noreply@example.com
      to_addresses:
        - customer@example.com
      subject_template: "Welcome, {{ row.first_name }}!"
      body_html: true
      body_template: |
        <h1>Hi {{ row.first_name }},</h1>
        <p>Thanks for signing up!</p>
        """
from __future__ import annotations
 
import json
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any
 
from drt.config.models import DestinationConfig, EmailDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template