"""IAMC data template implemented in SDMX.

IAMC defines a spreadsheet format: https://data.ene.iiasa.ac.at/database/.
Observations are stored in a 'data' sheet; “The columns included on this sheet
are Model, Scenario, Region, Variable, Unit, and any number of years.”

Using the SDMX information model (SDMX-IM or IM):

- Model, Scenario, Region, and Variable are *dimensions* appearing in single
  columns.
- Year is a *dimension* not named in the file, values of which are given by
  column titles.
- Unit is an *attribute*; it contains extra information associated with
  observations (or collections of observations).

This code demonstrates two things:

1. Programmatically creating a DataStructureDefinition (DSD) that encodes the
   IAMC data template (or a subset thereof).

2. Reading in some example data that is structured by that DSD, and exposing it
   as a pandas object.

Further comments inline.

iTEM also defines a data template (https://transportenergy.org/database/) with
the additional dimensions: Mode, Technology, Fuel. Could a single codebase
cover IAMC, iTEM, and possibly other uses?

"""
import csv
from itertools import tee
from pathlib import Path
import sys

import yaml

# Uses the code on the https://github.com/khaeru/pandaSDMX/tree/bare-ds branch
sys.path.append(str(Path('..', '..', 'pandaSDMX').resolve()))

import pandasdmx  # noqa: E402
from pandasdmx import model  # noqa: E402


# from https://docs.python.org/3/library/itertools.html#itertools-recipes
def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


# Open a file with some information about the IAMC data format
iamc_info = yaml.load(open('iamc.yaml'))

# Create the data structure definition
iamc = model.DataStructureDefinition(id='IAMC', name='IAMC data structure')

# Concepts
#
# The SDMX-IM abstracts the idea of a 'Concept'. In multiple data structures
# or datasets, the same Concept might appear as a Dimension or Attribute. We
# first define the concepts.
#
cs = model.ConceptScheme()

for concept in iamc_info['concepts']:
    cs.setdefault(**concept)

# Codes for variables
#
# The VARIABLE Concept is enumerated by a predefined set of values; in the
# SDMX-IM, these are termed 'Codes', which may have a hierarchy.
#
variables = model.Codelist()

for codes in iamc_info['variables']:
    codes = codes.split('|')
    if len(codes) == 1:
        # A top-level code with no parent; any children added later
        variables.setdefault(id=codes[0], name=codes[0])
    else:
        # Walk down: L1, L2, L3 → (L1, L2), (L2, L3)
        for parent, child in pairwise(codes):
            # Add the child to the Codelist and as a child of its parent
            variables.setdefault(id=child, name=child, parent=parent)

# The 'VARIABLE' concept is represented by these codes
cs['VARIABLE'].core_representation = model.Representation(enumerated=variables)

# Add dimensions to the DSD
for id, concept_id in iamc_info['dimensions'].items():
    dim = model.Dimension(id=id, concept_identity=cs[concept_id])
    iamc.dimensions.append(dim)

# A distinction that's minor in this example: the 'core' and 'local'
# representations of a concept may differ. For instance, in a large dataset,
# REGION might be enumerated by a long list of country codes ('core'), yet in a
# particular data message (like the one below), perhaps only one region
# appears, so it could be represented as a string ('local').
#
# Here, we use the same Codelist for the variables
iamc.dimensions[3].local_representation.enumerated = variables

# Add attributes to the DSD
for id, concept_id in iamc_info['attributes'].items():
    da = model.DataAttribute(id=id, concept_identity=cs[concept_id])
    iamc.attributes.append(da)

# At this point the DSD is complete.


# An example dataset, using plot_data.csv from pyam
ds = model.DataSet(structured_by=iamc)

# This is a crude CSV reader that wouldn't be used in production; it only
# works for the example data and doesn't perform strict checking. Its purpose
# is to demonstrate how the IAMC format maps into the SDMX information model.
#
with open('plot_data.csv') as f:
    reader = csv.DictReader(f)

    # The IAMC format is 'wide', with YEAR pivoted to columns. We represent
    # each row with a distinct SDMX-IM SeriesKey.
    for row in reader:
        # Get the Code for VARIABLE by walking the hierarchy. If the variable
        # string is invalid (i.e. contains an undefined Code, or has them out
        # of the correct hierarchy), this will fail.
        parent = None
        for v in row.pop('variable').split('|'):
            variable = parent.get_child(v) if parent else variables[v]
            assert parent is None or variable.parent is parent
            parent = variable

        # Construct the series key for all observations in this row
        sk = model.SeriesKey(VARIABLE=variable,
                             **{k.upper(): row.pop(k) for k in
                                ['model', 'scenario', 'region']})

        # Store the unit. An alternate way to do this would be to define one
        # GroupKey for each VARIABLE, and attach this attribute at the group
        # level. For this dataset, all variables happen to have the same units;
        # but in general that will not be the case.
        sk.attrib['UNIT'] = model.AttributeValue(
            value_for=iamc.attributes.get('UNIT'),
            value=row.pop('unit'),
            )

        # Prepare a list of Observation instances. YEAR is the dimension that
        # varies at the observation level; other dimensions are specified by
        # the SeriesKey for this row.
        obs = []
        for k, v in row.items():
            o = model.Observation(dimension=model.Key(YEAR=k), value=v,
                                  series_key=sk)
            obs.append(o)

        # Add the observations to the dataset
        ds.add_obs(obs, sk)


# Convert SDMX objects to a pd.Series
data = pandasdmx.to_pandas(ds)
print(data)

# Pick a subset of the data. pyam's filter() logic could be applied either to
# this pd.Series, or to the core pandaSDMX.
print(data.xs('test_model1', level='MODEL', drop_level=False).unstack('YEAR'))

# Show that the UNIT attribute is retained for each SeriesKey
print(list(sk.attrib for sk in ds.series.keys()))
