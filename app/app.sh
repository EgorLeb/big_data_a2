#!/bin/bash
# Start ssh server
service ssh restart 

bash init-cassandra.sh

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
if [ ! -f ".venv.tar.gz" ]; then
    venv-pack -o .venv.tar.gz
else
    echo "Virtual environment already packed. Skipping..."
fi

# Collect data
bash prepare_data.sh


# Run the indexer
bash index.sh

# Run the ranker
bash search.sh "which is a diatonic semitone above A and G"

echo "Services started. Holding the container open..."
tail -f /dev/null
