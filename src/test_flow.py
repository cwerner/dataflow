from prefect.artifacts import create_link, create_markdown

# publish a markdown artifact
create_markdown("# Hello!\n Place markdown text here.")

# publish a link artifact
create_link("http://prefect.io/")
