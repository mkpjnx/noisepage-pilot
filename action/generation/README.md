## GaRBAGE -- GlobAl Rule-Based Action Generation & Enumeration
GaRBAGE is an action enumeration system that will guide the search and planning of actions in NoisePage.
The space of all actions a self-driving DBMS can take to improve performance, e.g. building indexes, tuning knobs, adding MVs, is far too vast to search exhaustively and can change as the schema evolve.
Based on configurable "rules", GaRBAGE can restrict or expand the search space as needed -- either to help generate training data for system models, or as input into the search and planning modules."

## Configuration:
GaRBAGE is configurable via yaml files that provide the engine with the requisite information. 
Currently, this means supplying the engine with:
- A `Connector` to a postgres database
- `Workloads` defined by csvlogs that are used by the action generation rules 
- `Generators` or rules that dictate what actions are produced.

Examples can be found in the `sample_config` subfolder.

## Running GaRBAGE
Generate a list of actions based on the configuration by running
```python engine.py -o {OUTPUT}.json -c {CONFIG}.yaml```

The resultant `json` file contains a list of `Configurations` (e.g. Knobs, Indexes)
and corresponding actions that can be applied to them. This can then be used as an
input for tuning algorithms by defining their search space.

## Extending GaRBAGE

