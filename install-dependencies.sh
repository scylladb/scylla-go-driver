GO_PKGS="
golangci-lint       github.com/golangci/golangci-lint/cmd/golangci-lint
"

echo "==> Installing Go packages at ${LOCAL_BIN}"

function install() {
    echo "$1 ($2)"

    pushd mod > /dev/null
    go build -mod=readonly -o "${LOCAL_BIN}/$1" ${2}
    popd > /dev/null
}

pkgs=($(echo "${GO_PKGS}" | sed 's/\s+/\n/g'))
for i in "${!pkgs[@]}"; do
    if [[ $(($i % 2)) == 0 ]]; then
        install ${pkgs[$i]} ${pkgs[$((i+1))]}
    fi
done

echo "==> Complete!"