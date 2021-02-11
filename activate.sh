function activate_venv() {
{ # try: activate from the conda environment:
        conda activate mailizer
    } || { # catch: error
        echo ""; echo "ERROR: failed to activate virtual environment .venv! do it yourself"; return 1
    }
}

activate_venv && (
    echo ""
    echo "************************************************************************************"
    echo "Successfuly activated the virtual environment; you are now using this python:"
    echo "$ which python"
    echo "$(which python)"
    echo "************************************************************************************"
    echo ""
)
export PYTHONPATH="$(pwd):$PYTHONPATH"
export JUPYTER_PATH="$(pwd):$JUPYTER_PATH"