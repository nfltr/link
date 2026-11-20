module github.com/nfltr/link/pathswitch

go 1.27.0

// Its tests' benchmark runs a link Mux; the package itself is standard
// library only.
replace github.com/nfltr/link => ../

require github.com/nfltr/link v0.13.2
