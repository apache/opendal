vim.g.project_lspconfig = {
    rust_analyzer = {
        settings = {
            ["rust-analyzer"] = {
                cargo = {
                    features = { "services-redb" },
                },
            },
        },
    },
}
