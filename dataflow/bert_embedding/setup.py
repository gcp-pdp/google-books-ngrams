import setuptools

setuptools.setup(
    name="bert-embeddings",
    version="0.1.0",
    install_requires=["tensorflow-hub", "tensorflow-text", "bert-for-tf2"],
    packages=setuptools.find_packages(),
)
