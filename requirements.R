# Set CRAN repository
options(repos = c(CRAN = "https://cloud.r-project.org"))

# List of required packages
required_packages <- c(
  "DBI",
  "RPostgres",
  "dotenv",
  "jsonlite"
)

# Install missing packages
installed_packages <- rownames(installed.packages())
for (pkg in required_packages) {
  if (!(pkg %in% installed_packages)) {
    install.packages(pkg)
  }
}