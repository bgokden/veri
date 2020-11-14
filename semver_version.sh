#!/usr/bin/env bash

version_input="git"
version_output="git"
version_increment_type=""
version_prefix="v"
version_file_language="go"
version_file="./semantic_version/version.go"
version_message=""
version_bash_file="./version_env.sh"

increment_semantic_version() {
  local increment_type=$1
  local current_version=$( echo $2 | tr -dc '0-9.' )
  local version_array=( ${current_version//./ } )
  case $increment_type in
    "major" )
      ((version_array[0]++))
      version_array[1]=0
      version_array[2]=0
      ;;
    "minor" )
      ((version_array[1]++))
      version_array[2]=0
      ;;
    "patch" )
      ((version_array[2]++))
      ;;
  esac

  echo "${version_array[0]}.${version_array[1]}.${version_array[2]}"
}

get_incremented_semantic_version () {
  local increment_type=$1
  local current_version=$2
  if [ -z "$current_version" ]
  then
        current_version="0.0.0"
  fi
  local semantic_version=$(increment_semantic_version $increment_type $current_version)
  echo "${version_prefix}$semantic_version"
}

get_semantic_version_from_git () {
  local latest_tag=$(git rev-list --tags --max-count=1)
  if [ -z "$latest_tag" ]
  then
        echo ""
        return
  fi
  local git_version=$(git describe --tags $latest_tag)
  echo "$git_version"
}

set_semantic_version_to_git () {
  local tag_version="$1"
  local tag_message="$2"
  if [ -z "$tag_message" ]
  then
        tag_message="Release for version ${NEW_VERSION}"
  fi
  git tag -a "${tag_version}" -m "${tag_message}"
  git push origin "${tag_version}"
}

get_increment_semantic_type_from_git() {
  local latest_commit_message="$(get_commit_message_from_git)"
  echo $(get_increment_semantic_type_from_string "$latest_commit_message")
}

get_release_message_from_git() {
  echo $(get_commit_message_from_git)
}

get_commit_message_from_git() {
  echo $(git log -1 --pretty=%B)
}

get_increment_semantic_type_from_string() {
  local message="$1"
  if [[ $message == *"[major]"* ]]; then
      echo "major"
  elif [[ $message == *"[minor]"* ]]; then
      echo "minor"
  else
      echo "patch"
  fi
}

mkPath() { mkdir -p "$(dirname "$1")" || return; touch $1; }

create_go_version_file() {
  local file_path=$1
  local version_text=$2
  mkPath $1
  cat > $file_path <<EOF
package version
// This package is auto generated please don't edit manually

const Version string = "$version_text"
EOF
}

create_text_version_file() {
  local file_path=$1
  local version_text=$2
  mkPath $1
  echo "$version_text" > $file_path
}

create_bash_version_file() {
  local file_path=$1
  local version_text=$2
  mkPath $1
  local file_path=$1
  local version_text=$2
  mkPath $1
  cat > $file_path <<EOF
#!/usr/bin/env bash
# This package is auto generated please don't edit manually

export VERSION="$version_text"
EOF
}



while getopts ":i:o:t:p:f:l:m:b:" opt; do
  case ${opt} in
    i )
      version_input=$OPTARG
      ;;
    o )
      version_output=$OPTARG
      ;;
    t )
      version_increment_type=$OPTARG
      ;;
    p )
      version_prefix=$OPTARG
      ;;
    l )
      version_file_language=$OPTARG
      ;;
    f )
      version_file=$OPTARG
      ;;
    m )
      version_message=$OPTARG
      ;;
    b )
      version_bash_file=$OPTARG
      ;;
    \? )
      echo "Invalid option: $OPTARG" 1>&2
      ;;
    : )
      echo "Invalid option: $OPTARG requires an argument" 1>&2
      ;;
  esac
done
shift $((OPTIND -1))

echo "Reading current version from $version_input"
case ${version_input} in
  git )
    old_version=$(get_semantic_version_from_git)
    ;;
esac
echo "Current version is $old_version"
if [ -z "$version_message" ]
then
  case ${version_input} in
    git )
      version_message="$(get_release_message_from_git)"
      ;;
  esac
fi

if [ -z "$version_increment_type" ]
then
  case ${version_input} in
    git )
      version_increment_type=$(get_increment_semantic_type_from_git)
      ;;
  esac
fi

echo "Incrementing version due to $version_increment_type"
new_version=$(get_incremented_semantic_version $version_increment_type $old_version)
echo "New version will be $new_version with message: $version_message"

case ${version_output} in
  git )
    $(set_semantic_version_to_git "$new_version" "$version_message")
    ;;
esac

if [ ! -z "$version_file_language" ]
then
  echo "Create new version file $version_file for $version_file_language"
  case ${version_file_language} in
    go )
      create_go_version_file $version_file $new_version
      ;;
    text )
      create_text_version_file $version_file $new_version
      ;;
    bash )
      create_bash_version_file $version_file $new_version
      ;;
  esac
fi

create_bash_version_file $version_bash_file $new_version
echo $new_version
