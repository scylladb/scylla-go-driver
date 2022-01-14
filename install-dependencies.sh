GO_PKGS="
golangci-lint       github.com/golangci/golangci-lint/cmd/golangci-lint
"
local_bin="~/work/scylla-go-driver/scylla-go-driver/bin"

echo "==> Installing Go packages at ${PATH}"

function install() {
    echo "$1 ($2)"

    go build -mod=readonly -o "${local_bin}/$1" ${2}
}

pkgs=($(echo "${GO_PKGS}" | sed 's/\s+/\n/g'))
for i in "${!pkgs[@]}"; do
    if [[ $(($i % 2)) == 0 ]]; then
        install ${pkgs[$i]} ${pkgs[$((i+1))]}
    fi
done

echo "==> Complete!"
