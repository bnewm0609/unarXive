""" Normalize and parse.
"""

from argparse import ArgumentParser
import os
import shutil
import sys
import tarfile
import tempfile
import time
from normalize_arxiv_dump import normalize
from parse_latex_tralics import parse

# from parse_latex_tralics_fulltext_deprecated import parse as parse_fulltext
from parse_latex_tralics_fulltext import parse as parse_fulltext


def prepare_gz(
    in_dir_or_file,
    out_dir,
    meta_db,
    tar_fn_patt,
    write_logs=False,
    should_parse_fulltext=True,
    tralics_dir=None,
):
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    done_log_path = os.path.join(out_dir, "done.log")

    if os.path.isdir(in_dir_or_file):
        in_dir = in_dir_or_file
        # ext_sample = [os.path.splitext(fn)[-1] for fn in os.listdir(in_dir)[:10]]
        # if ".gz" not in ext_sample:
        #     print("input directory doesn't seem to contain GZIP archives")
        #     return False

        done_tars = []
        if os.path.isfile(done_log_path):
            with open(done_log_path) as f:
                lines = f.readlines()
            done_tars = [l.strip() for l in lines]

        gz_fns = [fn for fn in os.listdir(in_dir) if tar_fn_patt in fn]
    else:
        if not os.path.isdir(in_dir_or_file):
            print("input directory or file does not exist")
            return False
    gz_total = len(gz_fns)
    num_pdf_total = 0
    num_files_total = 0
    # for gz_idx, gz_fn in enumerate(gz_fns):
    # for each tar archive
    # print("{}/{} ({})".format(tar_idx + 1, gz_total, tar_fn))
    # if tar_fn in done_tars:
    #     print("done in a previous run. skipping")
    #     continue
    # tar_path = os.path.join(in_dir, tar_fn)

    with tempfile.TemporaryDirectory() as tmp_dir_path:
        # prepare folder for intermediate results
        tmp_dir_norm = os.path.join(tmp_dir_path, "normalized")
        os.mkdir(tmp_dir_norm)
        # "gracefully" handle input file access (currently a network mount)
        # containing_dir = os.listdir(tmp_dir_gz)[0]
        # containing_path = os.path.join(tmp_dir_gz, containing_dir)
        # for gz_fn in os.listdir(in_dir):
        #     num_files_total += 1
        #     gz_path_tmp = os.path.join(in_dir, gz_fn)
        #     if os.path.splitext(gz_fn)[-1] == ".pdf":
        #         num_pdf_total += 1
        #         os.remove(gz_path_tmp)
        #         continue
        #     gz_path_new = os.path.join(tmp_dir_gz, gz_fn)
        #     shutil.move(gz_path_tmp, gz_path_new)

        # adjust in_dir
        # Ben added
        # breakpoint()
        for fn in os.listdir(in_dir):
            full_path = in_dir + f"/{fn}"
            # if os.path.isdir(full_path) and len(os.listdir(tmp_dir_gz)) == 1:
            #     tmp_dir_gz = full_path

        source_file_info = normalize(in_dir, tmp_dir_norm, write_logs=write_logs)

        # just used to determine the name of the jsonl to save to
        # breakpoint()
        tar_fn = f"{os.path.basename(os.path.normpath(in_dir))}.tar"
        if should_parse_fulltext:
            parse_fulltext(
                tmp_dir_norm,
                out_dir,
                tar_fn,
                source_file_info,
                meta_db,
                incremental=False,
                write_logs=write_logs,
                tralics_dir=tralics_dir,
            )
        else:
            parse(
                tmp_dir_norm,
                out_dir,
                tar_fn,
                source_file_info,
                meta_db,
                incremental=False,
                write_logs=write_logs,
            )
    with open(done_log_path, "a") as f:
        f.write("{}\n".format(tar_fn))
    print("{} files".format(num_files_total))
    print("{} PDFs".format(num_pdf_total))


def prepare(
    in_dir_or_file,
    out_dir,
    meta_db,
    tar_fn_patt,
    write_logs=False,
    should_parse_fulltext=True,
    tralics_dir=None,
):
    """Processes a directory containing .tar files or a single .tar file"""
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    done_log_path = os.path.join(out_dir, "done.log")

    if os.path.isdir(in_dir_or_file):
        in_dir = in_dir_or_file
        ext_sample = [os.path.splitext(fn)[-1] for fn in os.listdir(in_dir)[:10]]
        if ".tar" not in ext_sample:
            print("input directory doesn't seem to contain TAR archives")
            return False

        done_tars = []
        if os.path.isfile(done_log_path):
            with open(done_log_path) as f:
                lines = f.readlines()
            done_tars = [l.strip() for l in lines]

        tar_fns = [fn for fn in os.listdir(in_dir) if tar_fn_patt in fn]
    elif os.path.isfile(in_dir_or_file):
        in_dir = os.path.dirname(in_dir_or_file)
        if ".tar" != os.path.splitext(in_dir_or_file)[-1]:
            print("input file must be a TAR archive")
            return False
        tar_fns = [os.path.basename(in_dir_or_file)]
        done_tars = []
    else:
        if not os.path.isdir(in_dir_or_file):
            print("input directory or file does not exist")
            return False
    tar_total = len(tar_fns)
    num_pdf_total = 0
    num_files_total = 0
    for tar_idx, tar_fn in enumerate(tar_fns):
        # for each tar archive
        print("{}/{} ({})".format(tar_idx + 1, tar_total, tar_fn))
        if tar_fn in done_tars:
            print("done in a previous run. skipping")
            continue
        tar_path = os.path.join(in_dir, tar_fn)
        # check if file can be skipped
        skip_file = False
        # "gracefully" handle input file access (currently a network mount)
        num_tries = 1
        while True:
            # try file access
            try:
                # try tar
                is_tar = False
                try:
                    is_tar = tarfile.is_tarfile(tar_path)
                except IsADirectoryError:
                    print(
                        ('unexpected directory "{}" in {}. skipping' "").format(
                            tar_fn, in_dir
                        )
                    )
                    skip_file = True
                if not is_tar:
                    print(('"{}" is not a TAR archive. skipping' "").format(tar_fn))
                    skip_file = True
                break  # not remote access problems
            except IOError as err:
                print(
                    ("[{}] IO error when trying check tar file: {}" "").format(
                        num_tries, err
                    )
                )
                num_tries += 1
                time.sleep(60)
        if skip_file:
            continue
        with tempfile.TemporaryDirectory() as tmp_dir_path:
            # prepare folders for intermediate results
            tmp_dir_gz = os.path.join(tmp_dir_path, "flattened")
            os.mkdir(tmp_dir_gz)
            tmp_dir_norm = os.path.join(tmp_dir_path, "normalized")
            os.mkdir(tmp_dir_norm)
            # extraxt
            # "gracefully" handle input file access (currently a network mount)
            num_tries = 1
            while True:
                try:
                    tar = tarfile.open(tar_path)
                    tar.extractall(path=tmp_dir_gz)
                    break
                except IOError as err:
                    print(
                        ("[{}] IO error when trying exract tar file: {}" "").format(
                            num_tries, err
                        )
                    )
                    num_tries += 1
                    time.sleep(60)
            containing_dir = os.listdir(tmp_dir_gz)[0]
            containing_path = os.path.join(tmp_dir_gz, containing_dir)
            for gz_fn in os.listdir(containing_path):
                num_files_total += 1
                gz_path_tmp = os.path.join(containing_path, gz_fn)
                if os.path.splitext(gz_fn)[-1] == ".pdf":
                    num_pdf_total += 1
                    os.remove(gz_path_tmp)
                    continue
                gz_path_new = os.path.join(tmp_dir_gz, gz_fn)
                shutil.move(gz_path_tmp, gz_path_new)
            os.rmdir(containing_path)
            # adjust in_dir
            # Ben added
            # breakpoint()
            for fn in os.listdir(tmp_dir_gz):
                full_path = tmp_dir_gz + f"/{fn}"
                if os.path.isdir(full_path) and len(os.listdir(tmp_dir_gz)) == 1:
                    tmp_dir_gz = full_path

            source_file_info = normalize(
                tmp_dir_gz, tmp_dir_norm, write_logs=write_logs
            )

            if should_parse_fulltext:
                parse_fulltext(
                    tmp_dir_norm,
                    out_dir,
                    tar_fn,
                    source_file_info,
                    meta_db,
                    incremental=False,
                    write_logs=write_logs,
                    tralics_dir=tralics_dir,
                )
            else:
                parse(
                    tmp_dir_norm,
                    out_dir,
                    tar_fn,
                    source_file_info,
                    meta_db,
                    incremental=False,
                    write_logs=write_logs,
                )
        with open(done_log_path, "a") as f:
            f.write("{}\n".format(tar_fn))
    print("{} files".format(num_files_total))
    print("{} PDFs".format(num_pdf_total))


if __name__ == "__main__":
    argp = ArgumentParser()
    argp.add_argument("in_dir_or_file", type=str)
    argp.add_argument("out_dir", type=str)
    argp.add_argument("metadata_db_path", type=str)
    argp.add_argument("--parse_fulltext", action="store_true")
    argp.add_argument("--tralics_dir", type=str)
    argp.add_argument("--gz", action="store_true")
    args = argp.parse_args()
    # if len(sys.argv) not in [4, 5]:
    #     print(
    #         (
    #             "usage: python3 prepare.py </path/to/in/dir> </path/to/out/dir> "
    #             "</path/to/metadata.db> [--parse_fulltext]"
    #         )
    #     )
    #     sys.exit()
    in_dir_or_file = args.in_dir_or_file
    out_dir_dir = args.out_dir
    meta_db = args.metadata_db_path
    tar_fn_patt = ".tar"
    if args.parse_fulltext:
        print("Parsing fulltext!")
        should_parse_fulltext = True
    else:
        print("Not parsing full text. Add `--parse_fulltext` to parse full text")
        should_parse_fulltext = False
    # if len(sys.argv) == 5:
    #     tar_fn_patt = sys.argv[4]
    # else:
    #     tar_fn_patt = ".tar"
    if args.gz:
        ret = prepare_gz(
            in_dir_or_file,
            out_dir_dir,
            meta_db,
            tar_fn_patt,
            write_logs=True,
            should_parse_fulltext=should_parse_fulltext,
            tralics_dir=args.tralics_dir,
        )
    else:
        ret = prepare(
            in_dir_or_file,
            out_dir_dir,
            meta_db,
            tar_fn_patt,
            write_logs=True,
            should_parse_fulltext=should_parse_fulltext,
            tralics_dir=args.tralics_dir,
        )
