from setuptools import setup, find_packages

setup(
    name="measurement_plane",
    version="0.2.1",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "python-dateutil>=2.8.0",
        "jsonschema>=4.0.0",
        "numpy>=1.21.0",
        "nats-py",   
    ],
    entry_points={
        'console_scripts': [
            'start-agent=measurement_plane.start_agent:start_agent',
        ],
    },
    include_package_data=True,
    description="Modular Measurement Plane for distributed experiments",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires='>=3.8',
)