"""Shared helpers for data paths and IO across DAGs."""
from pathlib import Path
from typing import Union

import pandas as pd


def project_root() -> Path:
    """Return the absolute project root path.

    Returns
    -------
    Path
        The absolute path to the project root directory.
    """
    return Path(__file__).resolve().parents[2]


def data_root() -> Path:
    """Ensure and return the repository `data` directory.

    Returns
    -------
    Path
        The path to the data directory, created if it doesn't exist.
    """
    root = project_root() / 'data'
    root.mkdir(parents=True, exist_ok=True)
    return root


def ensure_dir(path: Path) -> Path:
    """Create the parent directory of *path* if it does not exist.

    Parameters
    ----------
    path : Path
        The directory path to create.

    Returns
    -------
    Path
        The created directory path.
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_csv_file(path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """Read a CSV file from *path* into a DataFrame.

    Parameters
    ----------
    path : Union[str, Path]
        Path to the CSV file to read.
    **kwargs
        Additional keyword arguments passed to pandas.read_csv.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the CSV data.
    """
    return pd.read_csv(Path(path), **kwargs)


def read_parquet_file(path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """Read a Parquet file from *path* into a DataFrame.

    Parameters
    ----------
    path : Union[str, Path]
        Path to the Parquet file to read.
    **kwargs
        Additional keyword arguments passed to pandas.read_parquet.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the Parquet data.
    """
    return pd.read_parquet(Path(path), **kwargs)


def write_parquet_file(frame: pd.DataFrame, path: Union[str, Path]) -> str:
    """Write *frame* to Parquet at *path* and return the string path.

    Parameters
    ----------
    frame : pd.DataFrame
        DataFrame to write to Parquet format.
    path : Union[str, Path]
        Path where the Parquet file will be written.

    Returns
    -------
    str
        String representation of the output file path.
    """
    destination = Path(path)
    ensure_dir(destination.parent)
    frame.to_parquet(destination, index=False)
    return destination.as_posix()


def write_csv_file(frame: pd.DataFrame, path: Union[str, Path], **kwargs) -> str:
    """Write *frame* to CSV at *path* and return the string path.

    Parameters
    ----------
    frame : pd.DataFrame
        DataFrame to write to CSV format.
    path : Union[str, Path]
        Path where the CSV file will be written.
    **kwargs
        Additional keyword arguments passed to pandas.to_csv.

    Returns
    -------
    str
        String representation of the output file path.
    """
    destination = Path(path)
    ensure_dir(destination.parent)
    frame.to_csv(destination, index=False, **kwargs)
    return destination.as_posix()
