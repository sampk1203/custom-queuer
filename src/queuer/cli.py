import typer

app = typer.Typer(help="queuer: a serial background job queue")


@app.command()
def add():
    """Enqueue a job."""
    raise NotImplementedError


@app.command()
def list():
    """List queued, running, and recent jobs."""
    raise NotImplementedError


if __name__ == "__main__":
    app()
