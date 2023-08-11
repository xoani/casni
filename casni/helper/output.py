import sys


def colored(string: str, color: str='red') -> str:
    # Define color codes for basic color names
    color_codes = {
        'black': '0;30',
        'red': '0;31',
        'green': '0;32',
        'yellow': '0;33',
        'blue': '0;34',
        'magenta': '0;35',
        'cyan': '0;36',
        'white': '0;37',
        'reset': '0',
    }

    # If color is specified as an RGB tuple (r, g, b)
    if isinstance(color, tuple) and len(color) == 3:
        r, g, b = color
        color_code = f'38;2;{r};{g};{b}'
    else:
        # Use the color code from the dictionary if available
        color_code = color_codes.get(color.lower(), color_codes['red'])

    # Create the colored string using ANSI escape codes
    colored_string = f'\033[{color_code}m{string}\033[0m'
    return colored_string


def message(string, io='stdout'):
    if io == 'stdout':
        io = sys.stdout
    elif io == 'stderr':
        io = sys.stderr
    io.write(string)
    io.flush()