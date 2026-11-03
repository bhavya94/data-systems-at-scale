"""`dsas`: the one command. Each concept the repo adds becomes a subcommand here."""

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(prog="dsas", description="Run one concept and watch what it does.")
    parser.parse_args()
    parser.print_help()
