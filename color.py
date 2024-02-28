import tomllib

COLOR_SCHEMES_TOML_FILEPATH = "Q:\CMP\metrics\color_schemes.toml"


def load_color_scheme(
    scheme_name, color_schemes_toml_filepath=COLOR_SCHEMES_TOML_FILEPATH
):
    """Return the hex codes for a color scheme in color_schemes.toml"""
    with open(color_schemes_toml_filepath, "rb") as f:
        toml = tomllib.load(f)
    colors = toml["colors"]
    schemes = toml["schemes"]
    return [colors[color_name] for color_name in schemes[scheme_name]]
