# TPAU GTFS Utilities

### Requirements

- Anaconda

### Installation/Setup (Windows)

- Download repository by running `git clone git@github.com:anniekfifer/tpau-gtfsutils.git` in the CMD tool
- In the Anaconda Prompt application, run `initial-setup.bat` from the project root directory 

### Configuring and running a utility

- Edit the input parameters in the appropriate config yaml file in the `config/` folder. Follow formatting guidelines in the file's comments. 
- Copy or move any input GTFS is in the `data/` folder (GTFS in this folder will be read from, but not be altered in any way)
- In the Anaconda Prompt application, run `tpau-utils.bat` with the appropriate utility name (i.e., `tpau-utils.bat average_headway`) from the project root directory

### Configuring and running example

- Edit `config/average_headway.yaml` input parameters to use example values in comments
- Make sure that `good_feed.zip` (included in repo for testing) is in `data/`
- In the Anaconda Prompt application, run `tpau-utils.bat average_headway`
  
### Output
- Application output will go to `output/` directory
