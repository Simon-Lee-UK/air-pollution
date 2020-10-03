# Roadside Air Pollution Analysis
>A series of Jupyter notebooks assessing the impact of Covid-19 lockdown on air pollution readings 
from UK roadside sensors.  

An in-progress exploratory analysis of air pollution data from roadside sensors. Initial data processing pipeline has 
been completed by combining external package functions imported and consumed in Jupyter notebooks. I hope to identify 
and visualise impacts of Covid-19 lockdowns on air pollution readings at roadside sensors. For example, any shifts in 
distribution of pollution levels across weeks; general increase/decrease; rise/fall in particular kinds of pollutants; 
etc.  
  
I'm also interested to see whether a ML time-series analysis can predict future unseen years of air pollution data 
with appreciable accuracy. If so, it may be interesting to compare predicted vs. actual air pollution measurements 
for 2020.

## Getting Started
1\. Clone or download the repository:
```bash
git clone https://github.com/Simon-Lee-UK/air-pollution.git  # Grabs the code from GitHub
cd air-pollution  # Navigates into the top level of the repository
```

2\. Using the [Conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html) environment and package 
manager, create the environment required by the analysis notebooks and custom packages. First, create the environment 
from the `environment.yml` file:
```bash
conda env create -f environment.yml
```
Next, activate the newly created environment:
```bash
conda activate air-pollution-dev
```

3\. Run Jupyter Lab from the repository root:
```bash
jupyter lab
```

4\. Follow the instructions within notebooks to download, process and explore the example set of data or analyse data 
from a new roadside sensor.
  
## Extensions
Currently, data preview and read-in pipelines have been defined in custom package modules. These are generalised, making 
it simple to apply future notebook logic to new sets of air pollution data from different sensors.  
  
Still need to:
- Create new package module to handle local saving of processed data.
- Create exploratory graphs for example site data set incl. weekly distribution shifts pre-/post-C19 lockdown.
- Perform a time-series analysis to give a predicted set of data for 2020 based on previous years;
this can then be compared to the actual readings gathered in 2020.
  
## License
The code in this project is licensed under the [MIT License](https://choosealicense.com/licenses/mit/). 
See [LICENSE.txt](./LICENSE.txt) for details.

The air pollution data accessed in this project is licensed under the 
[Open Government Licence (OGL).](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/)
See [LICENSE.txt](./LICENSE.txt) for details.
