import os
from pathlib import Path
import requests
from scidownl import scihub_download
from stem import Signal
from stem.control import Controller


def get_current_ip():
    """Helper function to get the current IP address and test if Tor socket
    is working
    """
    session = requests.session()

    # TO Request URL with SOCKS over TOR
    session.proxies = {}
    session.proxies["http"] = "socks5h://localhost:9150"
    session.proxies["https"] = "socks5h://localhost:9150"

    try:
        r = session.get("http://httpbin.org/ip")
    except Exception as e:
        print(str(e))
    else:
        return r.text


def renew_tor_ip(password, port=9051) -> None:
    """Renew end node from Tor connection

    We want to renew the end node to avoid being blocked by Sci-Hub or any other
    request GET service. This function will renew the end node by sending a
    signal to the Tor controller to request a new identity.

    Before running this function, make sure that the Tor service is running and
    the Tor controller is enabled. This can be done by adding the following
    lines to the Tor configuration file:

    ```
    ControlPort 9051
    CookieAuthentication 1
    ```

    The password for the controller can be found in the `hash` file in the Tor
    data directory. The default location for the data directory is:

    - Linux: /var/lib/toror
    - macOS: /usr/local/etc/tor

    Parameters
    ----------
    password : str
        Password to authenticate with the Tor controller. This password is the
        one hashed in the Tor configuration

    port : int
        Port to connect to the Tor controller. Default is 9051.

    Returns
    -------
    None
    """

    with Controller.from_port(port=port) as controller:
        controller.authenticate(password=password)
        controller.signal(Signal.NEWNYM)

    return None


def download_paper(
    doi_url: str, title: str, lastname: str, path_pdfs: str | Path, **kwargs
) -> None:
    """Donwnload a paper from Sci-Hub using the DOI URL

    This function uses a Tor proxy to download the paper from Sci-Hub. For each
    request we use the Tor proxy on a new end node to avoid being blocked by
    Sci-Hub. The function will download the paper to the specified path.

    Parameters
    ----------
    doi_url : str
        DOI URL to download the paper from Sci-Hub
    title: str
        Title of the paper to download
    lastname: str
        Last name of the first author of the paper
    path_pdfs : str
        Path to save the downloaded PDF file
    **kwargs : dict
        Additional keyword arguments to pass to the `renew_tor_ip` function

    Returns
    -------
    None
    """
    file_name = f"{'_'.join([lastname, title.split(' ')[0]])}.pdf"
    out = os.path.join(path_pdfs, file_name)

    if not os.path.exists(out):
        proxy = {
            "http": "socks5://localhost:9050",
            "https": "socks5://localhost:9050",
        }
        # Renew the Tor IP address at each request! Proxy stays the same
        renew_tor_ip(**kwargs)
        scihub_download(doi_url, paper_type="doi", out=out, proxies=proxy)
    else:
        print(f"File {file_name} already exists")
