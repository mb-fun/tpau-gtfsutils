# TPAU GTFS Utilities

### Requirements

- Anaconda (https://www.anaconda.com/distribution/#download-section)

### Installation/Setup (Windows)

- Install all requirements listed above
- Download repository (`git clone git@github.com:anniekfifer/gtfsabm.git`)
- In project root directory, run:
  - `initial-setup.bat`

### Configuring and running a utility

- Edit the input parameters in the appropriate config yaml file in the `config/` folder. Follow formatting guidelines in the file's comments. 
- Copy or move any input GTFS is in the `data/` folder (GTFS in this folder will be read from, but not be altered in any way)
- To run a utility, run `tpau-utils.bat` with the appropriate utility name:
  - `tpau-utils.bat average_headway`

### Configuring and running example

- Edit `config/average_headway.yaml` input parameters to use example values in comments
- Make sure that `good_feed.zip` (included in repo for testing) is in `data/`
- Run `tpau-utils.bat average_headway`
  
### Output
- Application output will go to `output/` directory
