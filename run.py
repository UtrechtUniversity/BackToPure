import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from app import create_app

app = create_app()

if __name__ == '__main__':
    app.run(debug=True, port=5001)
