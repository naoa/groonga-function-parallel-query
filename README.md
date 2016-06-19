# Groonga parallel_query function

複数のクエリをマルチスレッドで並列に検索し、検索結果を直列にマージする。

レコード数が多すぎず、文書サイズが大きく検索にある程度時間がかかるようなケースで検索を早くすることができる。

たとえば、データサイズ：20GiB、 レコード数: 1750万、ヒット数：20万、ORが5つのクエリで通常0.57secかかるところが5並列させると、0.23secになる。

検索にあまり時間がかからず、2つめ以降のクエリの検索結果数が非常に多いようなケースではマージのコストの方が高くなり、遅くなることもある。

２つ目以降のクエリの検索結果が非常に多いと、マージのループが長くなるため、１つ目のクエリに検索結果が多くなるものをもってくるとよいかも。

デフォルトのワーカー数は8

環境変数
``GRN_PARALLEL_QUERY_N_WORKER``

検証中。

## Syntax

```
parallel_query(
 "match_columns", "query",
 "match_columns", "query",
 ...,
 ["AND|OR|NOT"]
)
```

最後の引数は、複数のクエリ同士をマージする際のoperator。デフォルトOR。先頭はスクリプト構文のoperatorが使われる。

``match_columns``で``columnA||columnB``など、複数カラムを``||``で区切った場合、自動的に展開されてパラレルに検索する。

## Install

Install libgroonga-dev.

Build this function.

    % sh autogen.sh
    % ./configure
    % make
    % sudo make install

## Usage

Register `functions/parallel_query`:

    % groonga DB
    > register functions/parallel_query

## Author

Naoya Murakami naoya@createfield.com

## License

LGPL 2.1. See COPYING-LGPL-2.1 for details.
