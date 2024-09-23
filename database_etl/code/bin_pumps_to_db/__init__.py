"""
Export the binary pump tables - solvent composition and timetable of a sample from its
.D dir. Options to access in runtime as pandas dataframes or export directly to database
under a 'bin_pump' schema.
"""

from .bin_pump_to_db_ import bin_pump_to_db as bin_pump_to_db
