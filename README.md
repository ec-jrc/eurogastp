![Documentation](https://img.shields.io/badge/python-3.8-blue.svg) [![License](https://img.shields.io/badge/License-EUPL--1.2-blue.svg)](https://opensource.org/licenses/EUPL-1.2)

# eurogastp - Python tools for analyzing the European gas system


## Description and features

This Python package facilitates downloading and processing data from the
following transparency platforms (TPs):
- ENTSOG     (https://transparency.entsog.eu/)
- GIE AGSI+  (https://agsi.gie.eu/)
- GIE ALSI   (https://alsi.gie.eu/)

In the case of the ENTSOG TP, the package does not only help with merely
downloading the data, but also with further processing it. The different
functions allow for
- periodizing and re-indexing (resampling the data with "gas days" as
  fixed frequency),
- selecting and filtering (selecting the data of interest), and
- aggregating (computing the sum over several data of interest)
the data. Due to the nature of the data and the format it comes in
(what ENTSOG calls the "compact" format), these steps are often required
for making best use of the data. In the case of the two GIE TPs, the data is
far less complex and those extra steps are not required. It is easy enough to
combine data from the GIE TPs with the (periodized and reindexed) data from
the ENTSOG TP.

The package makes use of the Pandas package, thus results of the function calls
are usually Pandas DataFrames.


## Quick start

We suggest to use `pip` to install eurogastp into your preferred Python
environment:

```bash
pip install eurogastp
```

If you want to download the very latest version from GitLab for use or
development purposes, you can of course also clone the repository directly
using Git:

```bash
git clone https://code.europa.eu/jrc-energy-security/eurogastp.git
```

In that case, you have to make sure that the newly created folder "eurogastp"
is added to your PYTHONPATH, so that you can `import eurogastp` inside your
preferred Python environment.

For development and application we have used the
[Anaconda Python Distribution](https://www.anaconda.com/distribution/) and
Python 3.8 on Windows 10. However, it should also work within other Python
environments.

To start off, there is a Jupyter notebook with a full example in the Examples
directory that you can follow along.


## Dependencies

eurogastp depends on the following 3rd-party Python packages:

pandas      ->      BSD-3-Clause Licence

numpy		->      BSD-3-Clause Licence

requests    ->      Apache 2.0 Licence

tqdm	    ->      MIT License

Python 		->      Python Software Foundation (PSF) License Agreement

openpyxl    ->      MIT License

pytables    ->      BSD 3-Clause License

eurogastp does not contain any code from these packages, neither in original
nor in modified form. They are merely software dependencies for the user to
run eurogastp on her own machine.

More details about the licenses of the 3rd-party software can be found in the
attached [Notice.txt](Notice.txt).


## Copyright notice

Copyright (C) 2022 European Union

Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
the European Commission – subsequent versions of the EUPL (the "Licence");
You may not use this work except in compliance with the Licence.
You may obtain a copy of the Licence at:

https://joinup.ec.europa.eu/software/page/eupl5

Unless required by applicable law or agreed to in writing, software
distributed under the Licence is distributed on an "AS IS" basis, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
Licence for the specific language governing permissions and limitations under
the Licence.


## Main developers

This software was initially developed by staff of the Joint Research Centre
(JRC) of the European Commission (EC), Petten, Netherlands.
 
Initial contributors are:

- Daniel Jung (EC-JRC) <daniel.jung@ec.europa.eu>
- Jean-Francois Vuillaume (EC-JRC) <Jean-Francois.VUILLAUME@ec.europa.eu>
- Ricardo Fernandez-Blanco Carramolino (EC-JRC) <Ricardo.CARRAMOLINO@ec.europa.eu>


## How to contribute

For ways to contribute please refer to the separate document [contributing.md](contributing.md).


## References

Jung, D., Vuillaume, J., Fernandez Blanco Carramolino, R., Filipe Calisto, H., Rodriguez Gomez, N. and Bolado Lavin, R., Tools for analysing the European natural gas system with public data – The Python package eurogastp, EUR 31281 EN, Publications Office of the European Union, Luxembourg, 2022, ISBN 978-92-76-58764-4, doi:10.2760/153669, JRC130771;
[https://publications.jrc.ec.europa.eu/repository/handle/JRC130771](https://publications.jrc.ec.europa.eu/repository/handle/JRC130771).
