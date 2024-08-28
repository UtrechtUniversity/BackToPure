import os
import sys


sys.path.insert(0, os.path.abspath('../../'))
sys.path.insert(0, os.path.abspath('../../voorheentests'))
sys.path.insert(0, os.path.abspath('../../src'))
sys.path.insert(0, os.path.abspath('../../other_files'))

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'B.O.S.S. Import to pure'
copyright = '2024, David Grote Beverborg'
author = 'David Grote Beverborg'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration
from unittest.mock import MagicMock
sys.modules['open'] = MagicMock()

extensions = [
    'sphinx.ext.autodoc',
    # other extensions...
]


templates_path = ['_templates']
exclude_patterns = []

html_logo = "logo.webp"
html_css_files = [
    'custom.css',
]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
