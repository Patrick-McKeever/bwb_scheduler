#!/bin/bash

# remote_server="128.208.252.232"
# remote_dir="/srv/lhhung/output"
# local_dir="$PWD/input"
# working_dir="$PWD/working"
# sshUser=lhhung
nthreads=1

# Check if the sshUser and number of threads are provided, unless localcopy is set
if [ -z "$localcopy" ]; then
    if [ -z "$sshUser" ] || [ -z "$nthreads" ]; then
        echo "Usage: $0 sshUser nthreads"
        exit 1
    fi
fi

# Check if ssh directory is present, unless localcopy is set
if [ -z "$localcopy" ]; then
    if [ -d "$HOME/.ssh" ]; then
        echo "ssh directory present"
    else
        echo "ssh directory not present - copying from $sshDir"
        cp -r "$sshDir" "$HOME/.ssh"
    fi
fi

# Create working directory if it doesn't exist
mkdir -p "$working_dir"

# Create local directory if it doesn't exist
mkdir -p "$local_dir"

# Function to check if the input directory is empty of files ending with inputExt
input_dir_empty() {
    local ext=$1
    if [ -z "$(ls -A "${local_dir}" | grep "${ext}$")" ]; then
        return 1 # input directory is empty
    else
        return 0 # input directory is not empty
    fi
}

# Function to check if the file is being copied or exists locally
file_status_check() {
    local file=$1
    if [ -d "${working_dir}/${file}" ] || [ -f "${local_dir}/${file}" ] || [ -f "${done_dir}/${file}" ]; then
        return 0 # file is being copied or exists locally or has been processed
    else
        return 1 # file is not being copied and does not exist locally
    fi
}

# Function to perform the file copy operation
copy_file() {
    local file=$1
    local file_name=$(basename "$file")
    local temp_file="${local_dir}/${file_name}.tmp"

    # Mark the file as being copied by creating a directory
    mkdir -p "${working_dir}/${file_name}"

    # Copy the file to a temporary file in local directory
    if [ -n "$localcopy" ]; then
        if cp "${file}" "${temp_file}"; then
            echo "File ${file_name} copied to local directory."
            # Move the temporary file to the final file name
            mv "${temp_file}" "${local_dir}/${file_name}"
            # Remove the placeholder directory on success
            rmdir "${working_dir}/${file_name}"
            return 0
        else
            echo "Failed to copy ${file_name}."
            # Remove the temporary file and placeholder directory if cp failed
            rm -f "${temp_file}"
            rmdir "${working_dir}/${file_name}"
            return 1
        fi
    else
        if scp "${sshUser}@${remote_server}:${file}" "${temp_file}"; then
            echo "File ${file_name} copied to local directory."
            # Move the temporary file to the final file name
            mv "${temp_file}" "${local_dir}/${file_name}"
            # Remove the placeholder directory on success
            rmdir "${working_dir}/${file_name}"
            return 0
        else
            echo "Failed to copy ${file_name}."
            # Remove the temporary file and placeholder directory if scp failed
            rm -f "${temp_file}"
            rmdir "${working_dir}/${file_name}"
            return 1
        fi
    fi
}

nwaits=0
# Main loop
while true; do
    # Wait until the input directory is empty
    nsleeps=0
    while input_dir_empty "$inputExt"; do
        # Echo if nsleeps is divisible by 6
        if [ $((nsleeps % 6)) -eq 0 ]; then
            # Find total time waited
            total_nonempty_time=$((nsleeps * 10))
            echo "Input directory is not empty. Slept for ${total_nonempty_time} seconds..."
        fi
        nsleeps=$((nsleeps + 1))        
        sleep 10
    done

    # Get the list of .fast5 or pod5 files on the remote server, unless localcopy is set
    if [ -z "$localcopy" ]; then
        file_list=$(ssh "${sshUser}@${remote_server}" "find '${remote_dir}' -name '*.$inputExt'")
        echo "Remote files found: $file_list"
    else
        cmd="find '${local_scan_dir}' -name '*.$inputExt'"
        echo "cmd: $cmd"
        file_list=$(eval "$cmd")
        echo "Local files found: $file_list"
    fi

    # Initialize files to be downloaded list
    filesToBeDownloaded=()

    # Check each file
    count=0

    for remote_file in $file_list; do
        file_name=$(basename "$remote_file")

        # Check if file is being copied or exists locally or has been processed
        if file_status_check "$file_name"; then
            continue
        else
            # Add file to the array and increment counter
            filesToBeDownloaded+=("$remote_file")
            ((count++))
        fi

        # Break the loop if the maximum number of files has been reached
        [ -n "$maxFiles" ] && [ $count -ge "$maxFiles" ] && break
    done

    # Download the files using up to nthreads if the list is not empty
    if [ ${#filesToBeDownloaded[@]} -gt 0 ]; then
        for file in "${filesToBeDownloaded[@]}"; do
            copy_file "$file" &
            if [ $(jobs -r -p | wc -l) -ge "$nthreads" ]; then
                # Wait for any copy operation to complete
                wait -n
            fi
        done

        # Wait for all remaining copy operations to complete
        wait
        echo "All downloads completed."
        exit 0
    else
        # Sleep and check again if the list is empty

        # Echo if nwaits is divisible by 6
        if [ $((nwaits % 6)) -eq 0 ]; then
            # Find total time waited
            total_time=$((nwaits * 10))
            echo "No files to download. Slept for ${total_time} seconds..."
        fi
        nwaits=$((nwaits + 1))
        sleep 10
    fi

done
