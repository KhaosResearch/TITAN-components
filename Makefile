clean:
    @rm -rf build dist .eggs *.egg-info
    @find . -type d -name '.mypy_cache' -exec rm -rf {} +
    @find . -type d -name '__pycache__' -exec rm -rf {} +

black: clean
    @isort --profile black drama_enbic2lab/
    @black drama_enbic2lab/

lint:
    @mypy drama_enbic2lab/

.PHONY: tests

tests:
    @python -m unittest discover -s drama_enbic2lab/ --quiet