# OpenMeteo ETL Documentation

This directory contains the MkDocs Material documentation for the OpenMeteo ETL Pipeline project.

## Building the Documentation Locally

### Prerequisites

Install MkDocs Material:

```bash
pip install mkdocs-material pymdown-extensions
```

### Preview Locally

From the `e2e_openmeteo` directory:

```bash
# Start development server with live reload
mkdocs serve

# Open http://127.0.0.1:8000 in your browser
```

### Build Static Site

```bash
# Build the site to the 'site' directory
mkdocs build

# Build with clean (removes old files)
mkdocs build --clean
```

### Deploy to GitHub Pages

```bash
# Deploy to gh-pages branch (automated via GitHub Actions)
mkdocs gh-deploy --force --clean
```

## Documentation Structure

- `index.md` - Main landing page with project overview
- `dashboard.md` - Dashboard showcase and setup
- `architecture.md` - System architecture and design
- `data_model.md` - Data warehouse schema
- `data_qulaity.md` - Data quality validation details
- `how_to_run.md` - Installation and deployment guide
- `credits.md` - Acknowledgments and references
- `stylesheets/custom.css` - Custom styling matching main portfolio

## Automatic Deployment

The documentation is automatically deployed to GitHub Pages when changes are pushed to the `main` branch via GitHub Actions workflow: `.github/workflows/deploy-openmeteo-docs.yml`

The site will be available at: `https://a-chmielewski.github.io/endtoend-etl-openmeteo/`

