# �u���Ώۂ̃f�B���N�g���i�K�v�ɉ����ĕύX�j
$targetPath = "C:\Users\seiji_yfc8940\final\final\docs\claude"  # �� ������ύX

# �u���O�ƒu����̕�����
$oldText = "error"
$newText = "repository"  # �C�ӂ̒u��������ɕύX�\

# �Ώۃt�@�C���̊g���q�i��: txt, md, cs �Ȃǁj
$extensions = @("*.md", "*.txt", "*.cs", "*.json", "*.yaml", "*.yml","*.mhtml")

# �Ώۃt�@�C�����ċA�I�Ɍ������A�u�����������s
Get-ChildItem -Recurse -Filter *.mhtml | ForEach-Object {
    $path = $_.FullName
    $content = Get-Content $path -Raw
    $filtered = $content -replace 'Snapshot-Content-Location:.*', ''
    Set-Content $path $filtered
    Write-Host "? Snapshot header removed from: $path"
}

