def lower_kebab_to_upper_snake_case(lower_kebab: str) -> str:
    return lower_kebab.replace("-", "_").upper()
