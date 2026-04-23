# rust_etl

Extensão PHP escrita em Rust para processamento paralelo de arquivos CSV em pipelines de ETL. Substitui fluxos baseados em `pcntl_fork` com performance 100-500x superior.

## Por que existe

ETLs em PHP tradicionalmente usam `pcntl_fork` para paralelizar o processamento de arquivos grandes. Isso tem limitações:

- Só funciona em CLI (não funciona em PHP-FPM)
- Cada fork copia a memória inteira do processo PHP (centenas de MB com Laravel bootado)
- Não é portável (Windows não tem `pcntl`)
- Tempo de boot dos workers é alto

Esta extensão resolve esses pontos usando threads nativas do Rust com a biblioteca `rayon`. Todo o pipeline (split, parse, transform, validate, merge) roda em Rust paralelo numa única chamada PHP.

## Performance

Benchmark de referência com arquivo CSV de 200MB e ~5M linhas:

| Etapa | PHP + pcntl | rust_etl |
|---|---|---|
| Split do arquivo | ~1-2s | 30ms |
| Processamento paralelo | ~30-60s | 75ms |
| Merge | ~500ms | 25ms |
| **Total** | **~40-70s** | **~130ms** |

## Requisitos

- PHP 8.0 a 8.4 (NTS ou ZTS)
- Rust toolchain (apenas para build)
- LLVM/Clang (para bindgen)

---

## Instalação

### macOS

#### 1. Dependências

```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# LLVM (necessário para o bindgen do ext-php-rs)
brew install llvm

# PHP com headers de desenvolvimento
brew install php@8.4
```

#### 2. Variáveis de ambiente

```bash
export PATH="$(brew --prefix php@8.4)/bin:$PATH"
export PHP_CONFIG="$(brew --prefix php@8.4)/bin/php-config"
export PHP="$(brew --prefix php@8.4)/bin/php"
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
```

#### 3. Configuração do linker (obrigatório no macOS)

Crie `.cargo/config.toml` na raiz do projeto:

```toml
[target.aarch64-apple-darwin]
rustflags = [
    "-C", "link-arg=-undefined",
    "-C", "link-arg=dynamic_lookup",
]

[target.x86_64-apple-darwin]
rustflags = [
    "-C", "link-arg=-undefined",
    "-C", "link-arg=dynamic_lookup",
]
```

#### 4. Build

```bash
cargo build --release
```

#### 5. Instalação

```bash
# Diretório para extensões
mkdir -p ~/php-ext
cp target/release/librust_etl.dylib ~/php-ext/rust_etl.so
```

#### 6. Configuração no php.ini

Descubra o arquivo ini carregado:

```bash
php --ini
```

Adicione ao `php.ini`:

```ini
extension_dir = "/Users/SEU_USUARIO/php-ext"
extension = rust_etl.so
```

#### 7. Verificação

```bash
php -m | grep rust_etl
php --re rust_etl
```

#### Observação para Laravel Herd

O Herd não distribui `php-config`, então use o PHP do Homebrew para compilar. Depois, copie a `.so` para o extension_dir usado pelo Herd e edite o `php.ini` em `~/Library/Application Support/Herd/config/php/<versao>/php.ini`.

### Linux (Ubuntu/Debian)

#### 1. Dependências

```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# PHP dev headers e libclang
sudo apt update
sudo apt install -y php8.4-dev libclang-dev build-essential pkg-config
```

#### 2. Build

```bash
cargo build --release
```

Gera `target/release/librust_etl.so`.

#### 3. Instalação

```bash
sudo cp target/release/librust_etl.so "$(php-config --extension-dir)/rust_etl.so"
```

#### 4. Configuração

Crie `/etc/php/8.4/mods-available/rust_etl.ini`:

```ini
extension=rust_etl.so
```

Habilite para CLI e FPM:

```bash
sudo phpenmod -v 8.4 rust_etl
sudo systemctl restart php8.4-fpm
```

#### 5. Verificação

```bash
php -m | grep rust_etl
```

### Notas de compilação

Binários **não são portáveis** entre:
- Arquiteturas diferentes (ARM64 macOS ≠ x86_64 Linux)
- Versões majors do PHP (8.2 ≠ 8.4)
- Builds NTS vs ZTS

Compile uma vez em cada ambiente de destino.

---

## Uso no PHP

A extensão expõe a classe global `FileProcessor` com quatro métodos.

### Instanciação

```php
$processor = new FileProcessor();
```

O construtor não recebe argumentos. Uma instância pode ser reutilizada para múltiplos arquivos.

### `splitFile(string $inputPath, string $outputDir, int $chunks): array`

Divide um arquivo CSV em N chunks, respeitando quebras de linha (nunca corta uma linha no meio). Cada chunk é escrito como `input_N.csv` no diretório de saída.

**Parâmetros:**

- `$inputPath` — caminho absoluto do arquivo de entrada
- `$outputDir` — diretório onde os chunks serão escritos (criado se não existir)
- `$chunks` — número de chunks/threads

**Retorno:** array com a contagem de linhas por chunk. Ex: `[349019, 349018, ...]`.

**Exemplo:**

```php
$processor = new FileProcessor();
$counts = $processor->splitFile(
    '/var/data/entrada.csv',
    '/tmp/etl_job_123',
    16
);

echo "Total de linhas: " . array_sum($counts);
// Cria /tmp/etl_job_123/input_0.csv até input_15.csv
```

### `processChunks(string $dir, int $chunks, string $inputDelimiter, string $outputDelimiter, bool $skipHeader, string $columnsJson): array`

Processa todos os chunks em paralelo aplicando transformações e validações definidas no layout. Lê `input_N.csv`, gera `output_N.csv` para cada chunk.

**Parâmetros:**

- `$dir` — diretório contendo os chunks gerados pelo `splitFile`
- `$chunks` — número de chunks (mesmo valor usado no split)
- `$inputDelimiter` — delimitador do CSV de entrada (ex: `";"`)
- `$outputDelimiter` — delimitador do CSV de saída (ex: `";"`)
- `$skipHeader` — se `true`, ignora a primeira linha de cada chunk
- `$columnsJson` — JSON serializado com a configuração de colunas (ver seção **Layout**)

**Retorno:** array com três inteiros: `[input_total, output_total, invalid_total]`.

- `input_total` — total de linhas lidas
- `output_total` — total de linhas válidas escritas
- `invalid_total` — total de linhas descartadas por falha de validação

**Exemplo:**

```php
$layout = json_encode([
    ['in' => 0, 'out' => 0, 'ops' => ['digits_only', 'pad_left:11:0'], 'validate' => 'cpf'],
    ['in' => 1, 'out' => 1, 'ops' => ['digits_only']],
    ['in' => 2, 'out' => 2, 'ops' => ['digits_only']],
]);

$totals = $processor->processChunks(
    '/tmp/etl_job_123',
    16,
    ';',
    ';',
    false,
    $layout
);

[$in, $out, $invalid] = $totals;
echo "Processadas: $in | Válidas: $out | Inválidas: $invalid";
```

### `mergeFiles(string $inputDir, string $outputPath, int $chunks): int`

Concatena `output_0.csv` até `output_N.csv` em um único arquivo final.

**Parâmetros:**

- `$inputDir` — diretório contendo os chunks processados
- `$outputPath` — caminho absoluto do arquivo final
- `$chunks` — número de chunks

**Retorno:** total de linhas no arquivo final.

**Exemplo:**

```php
$total = $processor->mergeFiles(
    '/tmp/etl_job_123',
    '/var/data/saida.csv',
    16
);

echo "Arquivo final gerado com $total linhas";
```

### `processFile(string $inputPath, string $outputPath, string $inputDelimiter, string $outputDelimiter, bool $skipHeader, string $columnsJson): array`

Versão single-shot que processa um arquivo inteiro (sem split/merge). Útil para arquivos menores ou quando não se deseja paralelizar via arquivos.

**Retorno:** `[input_count, output_count, invalid_count]`.

**Exemplo:**

```php
$totals = $processor->processFile(
    '/var/data/arquivo.csv',
    '/var/data/saida.csv',
    ';',
    ';',
    true,
    $layout
);
```

---

## Layout declarativo

O parâmetro `$columnsJson` define o pipeline de transformação de cada coluna. Formato:

```json
[
    {
        "in": 0,
        "out": 0,
        "ops": ["digits_only", "pad_left:11:0"],
        "validate": "cpf"
    },
    {
        "in": 1,
        "out": 1,
        "ops": ["digits_only"]
    }
]
```

### Campos

- **`in`** (obrigatório, int) — índice da coluna no CSV de entrada (0-based)
- **`out`** (obrigatório, int) — índice da coluna no CSV de saída (0-based)
- **`ops`** (obrigatório, array de strings) — lista ordenada de transformações
- **`validate`** (opcional, string) — validador aplicado após as transformações. Se falhar, a linha é descartada

### Operações disponíveis (`ops`)

| Operação | Descrição | Exemplo |
|---|---|---|
| `trim` | Remove espaços do início e fim | `" abc "` → `"abc"` |
| `digits_only` | Mantém apenas dígitos | `"(11) 98765-4321"` → `"11987654321"` |
| `uppercase` | Converte para maiúsculas | `"João"` → `"JOÃO"` |
| `lowercase` | Converte para minúsculas | `"João"` → `"joão"` |
| `pad_left:N:C` | Preenche à esquerda até N chars com o char C | `"123"` com `pad_left:5:0` → `"00123"` |
| `strip_ddi:DDI` | Remove o DDI do início da string | `"5511987654321"` com `strip_ddi:55` → `"11987654321"` |

As operações são aplicadas **na ordem declarada**. Em `["digits_only", "pad_left:11:0"]`, primeiro remove não-dígitos, depois preenche com zeros.

### Validadores disponíveis (`validate`)

| Validador | Regra |
|---|---|
| `cpf` | 11 dígitos, dígitos verificadores válidos, rejeita sequências repetidas (ex: `11111111111`) |
| `phone_br` | 10 ou 11 dígitos, DDD não começa com 0, celular (11 dígitos) deve ter `9` na terceira posição |

Se uma coluna com `validate` falhar, a **linha inteira** é descartada e contabilizada em `invalid_count`.

---

## Exemplo completo: pipeline ETL

```php
<?php

class EtlProcessor
{
    private FileProcessor $rust;
    private int $chunks;

    public function __construct(int $chunks = 16)
    {
        $this->rust = new FileProcessor();
        $this->chunks = $chunks;
    }

    public function process(string $inputFile, string $outputFile, array $layout): array
    {
        $workDir = sys_get_temp_dir() . '/etl_' . uniqid();
        mkdir($workDir, 0755, true);

        try {
            $t0 = microtime(true);

            // 1. Split paralelo
            $this->rust->splitFile($inputFile, $workDir, $this->chunks);

            // 2. Processamento paralelo
            $totals = $this->rust->processChunks(
                $workDir,
                $this->chunks,
                ';',
                ';',
                false,
                json_encode($layout)
            );

            // 3. Merge
            $this->rust->mergeFiles($workDir, $outputFile, $this->chunks);

            return [
                'input_count' => $totals[0],
                'output_count' => $totals[1],
                'invalid_count' => $totals[2],
                'elapsed_ms' => round((microtime(true) - $t0) * 1000, 2),
            ];
        } finally {
            $this->cleanup($workDir);
        }
    }

    private function cleanup(string $dir): void
    {
        foreach (glob("$dir/*") as $file) {
            unlink($file);
        }
        rmdir($dir);
    }
}

// Uso
$processor = new EtlProcessor(chunks: 16);

$result = $processor->process(
    inputFile: '/var/data/clientes.csv',
    outputFile: '/var/data/clientes_normalizados.csv',
    layout: [
        ['in' => 0, 'out' => 0, 'ops' => ['digits_only', 'pad_left:11:0'], 'validate' => 'cpf'],
        ['in' => 1, 'out' => 1, 'ops' => ['digits_only']],
        ['in' => 2, 'out' => 2, 'ops' => ['digits_only']],
    ]
);

print_r($result);
// Array
// (
//     [input_count] => 5584292
//     [output_count] => 5549000
//     [invalid_count] => 35292
//     [elapsed_ms] => 178.54
// )
```

---

## Integração com Laravel

Wrapper sugerido para substituir `FileHandler` + `Process` + `Pool` + `FileCache`:

```php
<?php

namespace App\Helpers\Parallel;

use App\Models\Enrichment\File;
use App\Models\Enrichment\Job;
use FileProcessor;
use RuntimeException;

class RustFileHandler
{
    private FileProcessor $processor;
    private Job $job;
    private File $inputFile;
    private string $inputPath;
    private string $outputPath;
    private int $workers;
    private int $maxWorkers = 16;

    public function __construct(Job $job, File $inputFile)
    {
        $this->processor = new FileProcessor();
        $this->job = $job;
        $this->inputFile = $inputFile;
        $this->inputPath = $inputFile->fullpath;

        $this->setWorkers();
        $this->setOutputPath();
    }

    public function run(array $layout): array
    {
        $this->cleanDirectory();

        $this->processor->splitFile(
            $this->inputPath,
            $this->getCacheInputPath(),
            $this->workers
        );

        $totals = $this->processor->processChunks(
            $this->getCacheInputPath(),
            $this->workers,
            ';',
            ';',
            false,
            json_encode($layout)
        );

        if ($totals[1] === 0) {
            throw new RuntimeException(
                'O arquivo de entrada não contém nenhum registro válido.'
            );
        }

        return [
            'input_record_count' => $totals[0],
            'output_record_count' => $totals[1],
            'invalid_record_count' => $totals[2],
        ];
    }

    public function mergeTo(string $finalOutputPath): int
    {
        return $this->processor->mergeFiles(
            $this->getCacheInputPath(),
            $finalOutputPath,
            $this->workers
        );
    }

    private function setWorkers(): void
    {
        $sizeInMB = (int) ceil(filesize($this->inputPath) / (1024 * 1024));
        $this->workers = min($sizeInMB, $this->maxWorkers);
    }

    private function setOutputPath(): void
    {
        $this->outputPath = $this->job
            ->firstFileOfType(File::TYPE_INPUT)
            ->getPathForParallel();

        if (!is_dir($this->outputPath)) {
            mkdir($this->outputPath, 0755, true);
        }
    }

    private function getCacheInputPath(): string
    {
        return $this->outputPath . '/input';
    }

    private function cleanDirectory(): void
    {
        $path = $this->getCacheInputPath();
        if (!is_dir($path)) {
            mkdir($path, 0755, true);
            return;
        }
        foreach (glob("$path/*") as $file) {
            unlink($file);
        }
    }
}
```

Uso no job:

```php
public function execute()
{
    $job = $this->getJob();
    $this->createEvent('Iniciou a conversão do arquivo de entrada');

    $handler = new RustFileHandler(
        $job,
        $job->firstFileOfType(File::TYPE_INPUT)
    );

    $result = $handler->run([
        ['in' => 0, 'out' => 0, 'ops' => ['digits_only', 'pad_left:11:0'], 'validate' => 'cpf'],
        ['in' => 1, 'out' => 1, 'ops' => ['digits_only']],
        ['in' => 2, 'out' => 2, 'ops' => ['digits_only']],
    ]);

    if ($result['output_record_count'] === 0) {
        throw new EnrichmentJobException(
            'O arquivo de entrada não contém nenhum registro válido.'
        );
    }

    $this->createEvent('Terminou a conversão do arquivo de entrada');
}
```

---

## Tratamento de erros

Os métodos da extensão lançam exceções PHP (`\Exception`) quando há falhas:

- Arquivo não existe ou sem permissão de leitura
- Diretório de saída sem permissão de escrita
- JSON de layout inválido
- Encoding UTF-8 inválido no arquivo (linhas problemáticas são puladas silenciosamente, sem erro)

```php
try {
    $processor->splitFile($path, $dir, 16);
} catch (\Exception $e) {
    Log::error('Falha no split: ' . $e->getMessage());
}
```

---

## Limitações

- **Encoding:** assume UTF-8. Linhas inválidas são ignoradas. Converta arquivos Latin1 antes de processar.
- **Memória:** usa `mmap` no arquivo de entrada. Arquivos maiores que a RAM disponível podem causar swap.
- **Callbacks PHP:** não suporta callbacks PHP dentro do processamento paralelo. Toda lógica deve ser expressa via layout declarativo.
- **Layouts customizados:** para transformações que não existem no conjunto padrão, é necessário estendê-las em Rust e recompilar.

---

## Troubleshooting

### `Unable to load dynamic library`

Confira se a `.so` foi copiada para o `extension_dir` correto:

```bash
php -i | grep extension_dir
```

### `undefined symbol: _zend_empty_string` no macOS

Falta a configuração do linker em `.cargo/config.toml`. Ver seção de instalação macOS.

### `The current version of PHP is not supported`

A versão do `ext-php-rs` usada no build não suporta sua versão do PHP. Para PHP 8.4, use a branch `master`:

```toml
ext-php-rs = { git = "https://github.com/davidcole1340/ext-php-rs", branch = "master" }
```

### Extensão carrega mas classe `FileProcessor` não existe

O `#[php_module]` não está registrando a classe. Confira:

```rust
#[php_module]
pub fn module(module: ModuleBuilder) -> ModuleBuilder {
    module.class::<FileProcessor>()
}
```

### Performance pior que o esperado

- Confirme que compilou com `--release`
- Confirme o número de threads vs cores disponíveis (`nproc` no Linux, `sysctl -n hw.ncpu` no macOS)
- Arquivos muito pequenos (<10MB) têm ganho menor — o overhead fixo domina